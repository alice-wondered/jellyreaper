package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	stdhttp "net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	bbolt "go.etcd.io/bbolt"
	"jellyreaper/internal/app"
	"jellyreaper/internal/discord"
	"jellyreaper/internal/jellyfin"
	bboltstore "jellyreaper/internal/repo/bbolt"

	"jellyreaper/internal/config"
	"jellyreaper/internal/domain"
	api "jellyreaper/internal/http"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/jobs/handlers"
	"jellyreaper/internal/repo"
	"jellyreaper/internal/scheduler"
	"jellyreaper/internal/worker"
)

const backfillCheckpointKey = "backfill.last_success_at"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	if err := os.MkdirAll(cfg.LogDir, 0o755); err != nil {
		logger.Error("create log directory", "error", err, "dir", cfg.LogDir)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.EmbedDir, 0o755); err != nil {
		logger.Error("create embed directory", "error", err, "dir", cfg.EmbedDir)
		os.Exit(1)
	}

	logFilePath := filepath.Join(cfg.LogDir, "jellyreaper.log")
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		logger.Error("open log file", "error", err, "path", logFilePath)
		os.Exit(1)
	}
	defer logFile.Close()

	logWriter := io.MultiWriter(os.Stdout, logFile)
	logger = slog.New(slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		logger.Error("create db directory", "error", err)
		os.Exit(1)
	}

	store, err := bboltstore.Open(cfg.DBPath, 0o600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		logger.Error("open store", "error", err)
		os.Exit(1)
	}
	defer store.Close()

	discordService, err := discord.NewService(cfg.DiscordBotToken, cfg.DiscordPublicKey)
	if err != nil {
		logger.Error("create discord service", "error", err)
		os.Exit(1)
	}
	discordService.SetEmbedPersistenceDir(cfg.EmbedDir)

	handlerList := []jobs.JobHandler{
		handlers.NewEvaluatePolicyHandler(store, logger),
		handlers.NewSendHITLPromptHandler(store, logger, discordService, cfg.DiscordChannelID, 48*time.Hour),
		handlers.NewHITLTimeoutHandler(store),
		handlers.NewExecuteDeleteHandler(store, jellyfin.NewClient(cfg.JellyfinURL, cfg.JellyfinAPIKey, nil)),
		handlers.NewNoopHandler(domain.JobKindVerifyDelete, logger),
		handlers.NewNoopHandler(domain.JobKindReconcileItem, logger),
	}

	registry, err := jobs.NewRegistry(handlerList...)
	if err != nil {
		logger.Error("create job registry", "error", err)
		os.Exit(1)
	}

	wakeCh := make(chan time.Time, 64)
	wake := func(at time.Time) {
		select {
		case wakeCh <- at:
		default:
		}
	}
	appService := app.NewService(store, logger, wake)
	dispatcher := worker.NewDispatcher(store, registry, logger)
	schedulerLoop := scheduler.NewLoop(store, dispatcher.Dispatch, logger, scheduler.Config{
		LeaseOwner: cfg.WorkerID,
		LeaseLimit: 32,
		LeaseTTL:   cfg.LeaseTTL,
		IdlePoll:   10 * time.Second,
		Signal:     wakeCh,
	})

	if len(cfg.DiscordPublicKey) == 0 {
		logger.Warn("DISCORD_PUBLIC_KEY_HEX is empty; /discord/interactions will reject requests")
	}

	server, err := api.NewServer(api.Config{
		Addr:                     cfg.HTTPAddr,
		Jellyfin:                 appService.HandleJellyfinWebhook,
		Discord:                  discordService,
		HandleDiscordInteraction: appService.HandleDiscordComponentInteraction,
	})
	if err != nil {
		logger.Error("build http server", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		logger.Info("scheduler started")
		if err := schedulerLoop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("scheduler stopped with error", "error", err)
		}
	}()

	if cfg.DiscordChannelID != "" {
		if err := discordService.SendSystemMessage(cfg.DiscordChannelID, "JellyReaper is online and ready."); err != nil {
			logger.Warn("failed to send online announcement", "error", err)
		}
	}

	if cfg.BackfillEnabled {
		if cfg.JellyfinURL == "" || cfg.JellyfinAPIKey == "" {
			logger.Warn("backfill enabled but jellyfin credentials are incomplete; skipping backfill startup")
		} else {
			backfillSvc, err := jellyfin.NewBackfillService(cfg.JellyfinURL, cfg.JellyfinAPIKey, nil)
			if err != nil {
				logger.Error("create backfill service", "error", err)
			} else {
				go runBackfillLoop(ctx, logger, store, appService, discordService, cfg, backfillSvc)
			}
		}
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("http shutdown failed", "error", err)
		}
	}()

	logger.Info("http server started", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, stdhttp.ErrServerClosed) {
		logger.Error("http server exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("shutdown complete")
}

func runBackfillLoop(ctx context.Context, logger *slog.Logger, repository repo.Repository, appService *app.Service, discordService *discord.Service, cfg config.Config, backfillSvc *jellyfin.BackfillService) {
	if cfg.DiscordChannelID != "" {
		if err := discordService.SendSystemMessage(cfg.DiscordChannelID, "Starting Jellyfin backfill and reconciliation run."); err != nil {
			logger.Warn("failed to send backfill start announcement", "error", err)
		}
	}

	run := func(isStartup bool) {
		since, err := resolveBackfillStart(ctx, repository, cfg)
		if err != nil {
			logger.Error("resolve backfill checkpoint", "error", err)
			return
		}

		startedAt := time.Now().UTC()
		plays, err := fetchPlaybackWithRetry(ctx, backfillSvc, since, cfg.BackfillLimit)
		if err != nil {
			logger.Error("backfill playback fetch failed", "error", err, "since", since)
			return
		}
		items, err := fetchChangedItemsWithRetry(ctx, backfillSvc, since, cfg.BackfillLimit)
		if err != nil {
			logger.Error("backfill item fetch failed", "error", err, "since", since)
			return
		}

		if err := appService.IngestBackfillPlayback(ctx, plays); err != nil {
			logger.Error("backfill playback ingest failed", "error", err)
			return
		}
		if err := appService.IngestBackfillItems(ctx, items); err != nil {
			logger.Error("backfill item ingest failed", "error", err)
			return
		}

		completedAt := time.Now().UTC()
		if err := saveBackfillCheckpoint(ctx, repository, completedAt); err != nil {
			logger.Error("save backfill checkpoint failed", "error", err)
		}

		logger.Info("backfill completed", "since", since, "plays_fetched", len(plays), "items_fetched", len(items), "duration", completedAt.Sub(startedAt).String())
		if isStartup && cfg.DiscordChannelID != "" {
			msg := "Backfill complete: plays=" + strconv.Itoa(len(plays)) + ", items=" + strconv.Itoa(len(items))
			if err := discordService.SendSystemMessage(cfg.DiscordChannelID, msg); err != nil {
				logger.Warn("failed to send backfill completion announcement", "error", err)
			}
		}
	}

	run(true)

	ticker := time.NewTicker(cfg.BackfillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run(false)
		}
	}
}

func resolveBackfillStart(ctx context.Context, repository repo.Repository, cfg config.Config) (time.Time, error) {
	now := time.Now().UTC()
	var raw string
	var exists bool
	if err := repository.WithTx(ctx, func(tx repo.TxRepository) error {
		value, ok, err := tx.GetMeta(ctx, backfillCheckpointKey)
		if err != nil {
			return err
		}
		raw, exists = value, ok
		return nil
	}); err != nil {
		return time.Time{}, err
	}
	if !exists || raw == "" {
		return now.Add(-cfg.BackfillLookback), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return now.Add(-cfg.BackfillLookback), nil
	}
	return parsed.Add(-cfg.BackfillOverlap), nil
}

func saveBackfillCheckpoint(ctx context.Context, repository repo.Repository, at time.Time) error {
	return repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, backfillCheckpointKey, at.UTC().Format(time.RFC3339Nano))
	})
}

func fetchPlaybackWithRetry(ctx context.Context, backfillSvc *jellyfin.BackfillService, since time.Time, limit int32) ([]jellyfin.PlaybackEvent, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		plays, err := backfillSvc.FetchPlaybackEventsSince(ctx, since, limit)
		if err == nil {
			return plays, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return nil, lastErr
}

func fetchChangedItemsWithRetry(ctx context.Context, backfillSvc *jellyfin.BackfillService, since time.Time, limit int32) ([]jellyfin.ItemSnapshot, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		items, err := backfillSvc.FetchChangedItemsSince(ctx, since, limit)
		if err == nil {
			return items, nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return nil, lastErr
}
