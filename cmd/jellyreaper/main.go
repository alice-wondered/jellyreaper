package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	stdhttp "net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	bbolt "go.etcd.io/bbolt"
	"jellyreaper/internal/ai"
	"jellyreaper/internal/app"
	"jellyreaper/internal/discord"
	"jellyreaper/internal/jellyfin"
	bboltstore "jellyreaper/internal/repo/bbolt"

	"jellyreaper/internal/config"
	"jellyreaper/internal/domain"
	api "jellyreaper/internal/http"
	"jellyreaper/internal/jobs"
	"jellyreaper/internal/jobs/handlers"
	"jellyreaper/internal/radarr"
	"jellyreaper/internal/repo"
	"jellyreaper/internal/scheduler"
	"jellyreaper/internal/sonarr"
	"jellyreaper/internal/worker"
)

const backfillCheckpointKey = "backfill.last_success_at"
const backfillCursorKey = "backfill.cursor.v1"

const (
	backfillRetryBaseDelay = time.Second
	backfillRetryMaxDelay  = 2 * time.Minute
	fetchRetryMaxAttempts  = 5
	fetchRetryBaseDelay    = 250 * time.Millisecond
	fetchRetryMaxDelay     = 30 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}
	if cfg.BackfillPlaybackEnabled {
		logger.Warn("BACKFILL_PLAYBACK_ENABLED is deprecated and ignored; backfill is item/userdata based")
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
	discordService.SetJellyfinImageSource(cfg.JellyfinURL, cfg.JellyfinAPIKey)
	var assistant *ai.Harness
	aiConfigured := strings.TrimSpace(cfg.AIModel) != "" && (strings.TrimSpace(cfg.AIAPIKey) != "" || strings.TrimSpace(cfg.AIBaseURL) != "")
	if aiConfigured {
		provider, err := ai.NewProvider(ai.ProviderConfig{
			Provider: cfg.AIProvider,
			APIKey:   cfg.AIAPIKey,
			BaseURL:  cfg.AIBaseURL,
		})
		if err != nil {
			logger.Error("create ai provider", "error", err, "provider", cfg.AIProvider)
			os.Exit(1)
		}
		assistant = ai.NewHarnessWithProvider(store, provider, cfg.AIModel)
		assistant.SetHistoryRestorer(func(ctx context.Context, threadID string, limit int) ([]string, error) {
			return discordService.LoadThreadHistory(ctx, threadID, limit)
		})
		discordService.SetMentionCallback(func(ctx context.Context, mention discord.MentionMessage) (string, error) {
			thread := mention.ThreadID
			if strings.TrimSpace(thread) == "" {
				thread = mention.ChannelID + ":msg:" + mention.MessageID
			}
			replyChannel := mention.ThreadID
			if replyChannel == "" {
				replyChannel = mention.ChannelID
			}
			typing := func() { _ = discordService.ChannelTyping(replyChannel) }
			return assistant.HandleMention(ctx, thread, mention.Author, mention.Content, typing)
		})
		logger.Info("ai mention assistant enabled", "provider", cfg.AIProvider, "model", cfg.AIModel)
	}
	if assistant != nil {
		if err := discordService.OpenGateway(); err != nil {
			logger.Warn("failed to open discord gateway for mention assistant", "error", err)
		} else {
			defer func() { _ = discordService.CloseGateway() }()
		}
	} else {
		logger.Info("discord gateway disabled (mention assistant not configured)")
	}

	evaluatePolicyHandler := handlers.NewEvaluatePolicyHandler(store, logger)
	evaluatePolicyHandler.SetDefaultExpireDays(cfg.DefaultLastPlayedThresholdDays)
	evaluatePolicyHandler.SetDefaultHITLTimeoutHours(cfg.DefaultHITLTimeoutHours)
	executeDeleteHandler := handlers.NewExecuteDeleteHandler(store, jellyfin.NewClient(cfg.JellyfinURL, cfg.JellyfinAPIKey, nil))
	executeDeleteHandler.SetDiscordService(discordService)
	hitlTimeout := time.Duration(cfg.DefaultHITLTimeoutHours) * time.Hour
	handlerList := []jobs.JobHandler{
		evaluatePolicyHandler,
		handlers.NewSendHITLPromptHandler(store, logger, discordService, cfg.DiscordChannelID, hitlTimeout),
		handlers.NewHITLTimeoutHandler(store, discordService, logger),
		executeDeleteHandler,
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
	appService.SetDiscordService(discordService)
	appService.SetPolicyDefaults(cfg.DefaultLastPlayedThresholdDays, cfg.DefaultDelayWindow)
	appService.SetDefaultHITLTimeoutHours(cfg.DefaultHITLTimeoutHours)
	if cfg.JellyfinURL != "" && cfg.JellyfinAPIKey != "" {
		appService.SetJellyfinClient(jellyfin.NewClient(cfg.JellyfinURL, cfg.JellyfinAPIKey, nil))
	}
	if cfg.RadarrURL != "" && cfg.RadarrAPIKey != "" {
		executeDeleteHandler.SetRadarrService(radarr.NewService(cfg.RadarrURL, cfg.RadarrAPIKey))
		logger.Info("radarr service enabled")
	}
	if cfg.SonarrURL != "" && cfg.SonarrAPIKey != "" {
		executeDeleteHandler.SetSonarrService(sonarr.NewService(cfg.SonarrURL, cfg.SonarrAPIKey))
		logger.Info("sonarr service enabled")
	}
	if assistant != nil {
		assistant.SetDecisionService(appService)
	}
	appService.SetBackfillWriteBatching(cfg.BackfillWriteBatchSize, cfg.BackfillWriteBatchTimeout, cfg.BackfillWriteQueueCapacity)
	dispatcher := worker.NewDispatcher(store, registry, logger)
	if cfg.DiscordChannelID != "" {
		dispatcher.SetDeleteFailedNotifier(func(ctx context.Context, flow domain.Flow, deleteErr error) {
			name := strings.TrimSpace(flow.DisplayName)
			if name == "" {
				name = flow.ItemID
			}
			msg := fmt.Sprintf("Delete FAILED for %s (item_id=%s): %v", name, flow.ItemID, deleteErr)
			logger.Error("delete terminal failure", "lex", "DELETION", "item_id", flow.ItemID, "error", deleteErr)
			if err := discordService.SendSystemMessage(cfg.DiscordChannelID, msg); err != nil {
				logger.Warn("failed to send delete failure notification", "lex", "DELETION", "item_id", flow.ItemID, "error", err)
			}
		})
	}
	schedulerLoop := scheduler.NewLoop(store, dispatcher.Dispatch, logger, scheduler.Config{
		LeaseOwner: cfg.WorkerID,
		LeaseLimit: 32,
		LeaseTTL:   cfg.LeaseTTL,
		IdlePoll:   10 * time.Second,
		Signal:     wakeCh,
	})
	schedulerObj := scheduler.NewScheduler(schedulerLoop, wake)
	evaluatePolicyHandler.SetEvalScheduler(schedulerObj)
	appService.SetEvalScheduler(schedulerObj)

	if len(cfg.DiscordPublicKey) == 0 {
		logger.Warn("DISCORD_PUBLIC_KEY_HEX is empty; /discord/interactions will reject requests")
	}

	server, err := api.NewServer(api.Config{
		Addr:                     cfg.HTTPAddr,
		Jellyfin:                 appService.HandleJellyfinWebhook,
		JellyfinWebhookToken:     cfg.JellyfinWebhookToken,
		Discord:                  discordService,
		HandleDiscordInteraction: appService.HandleDiscordComponentInteraction,
	})
	if err != nil {
		logger.Error("build http server", "error", err)
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if cfg.BackfillEnabled {
		if cfg.JellyfinURL == "" || cfg.JellyfinAPIKey == "" {
			logger.Warn("backfill enabled but jellyfin credentials are incomplete; skipping backfill startup")
		} else {
			backfillSvc, err := jellyfin.NewBackfillService(cfg.JellyfinURL, cfg.JellyfinAPIKey, nil)
			if err != nil {
				logger.Error("create backfill service", "error", err)
			} else {
				backfillSvc.SetWarningHook(func(stage string, warnErr error) {
					logger.Warn("backfill warning", "stage", stage, "error", warnErr)
				})
				backfillSvc.SetProgressHook(func(progress jellyfin.FetchProgress) {
					remaining := 0
					if progress.TotalRecordCount > 0 && progress.TotalRecordCount > progress.Fetched {
						remaining = progress.TotalRecordCount - progress.Fetched
					}
					logger.Info("backfill fetch progress",
						"stream", progress.Stream,
						"page", progress.Page,
						"page_items", progress.PageItems,
						"fetched", progress.Fetched,
						"total", progress.TotalRecordCount,
						"remaining", remaining,
						"since", progress.Since,
					)
				})

				logger.Info("running startup backfill before scheduler/http")
				if err := runBackfillOnce(ctx, logger, store, appService, discordService, cfg, backfillSvc, true); err != nil {
					logger.Warn("startup backfill run failed", "error", err)
				} else {
					logNextQueuedJob(ctx, logger, store)
				}

				go runBackfillLoop(ctx, logger, store, appService, discordService, cfg, backfillSvc, false)
			}
		}
	}

	// Reconcile flows that got stuck in a prior run (e.g. pending_review
	// with no HITL prompt, or active flows past their nextActionAt).
	if reconciled, err := appService.ReconcileStaleFlows(ctx); err != nil {
		logger.Warn("stale flow reconciliation failed", "error", err)
	} else if reconciled > 0 {
		logger.Info("reconciled stale flows on startup", "count", reconciled)
	}

	// Run periodic reconciliation so runtime failures (e.g. a terminal
	// send_hitl_prompt job) don't leave flows stuck until a restart.
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if reconciled, err := appService.ReconcileStaleFlows(ctx); err != nil {
					logger.Warn("periodic reconciliation failed", "error", err)
				} else if reconciled > 0 {
					logger.Info("periodic reconciliation recovered flows", "count", reconciled)
				}
			}
		}
	}()

	go func() {
		logger.Info("scheduler started")
		if err := schedulerLoop.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("scheduler stopped with error", "error", err)
		}
	}()

	// right now we send this in a bit of a race condition with the backfill messages that get sent, causing some weird status updates
	// I don't know that we really need this when we can just set the bot's status to online
	if cfg.DiscordChannelID != "" {
		if err := discordService.SendSystemMessage(cfg.DiscordChannelID, "JellyReaper is online and ready."); err != nil {
			logger.Warn("failed to send online announcement", "error", err)
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

// can we somehow combine this with the "run backfill once" logic? This feels like a weird split in our code
func runBackfillLoop(ctx context.Context, logger *slog.Logger, repository repo.Repository, appService *app.Service, discordService *discord.Service, cfg config.Config, backfillSvc *jellyfin.BackfillService, runStartup bool) {
	if cfg.DiscordChannelID != "" {
		if err := discordService.SendSystemMessage(cfg.DiscordChannelID, "Starting Jellyfin backfill and reconciliation run."); err != nil {
			logger.Warn("failed to send backfill start announcement", "error", err)
		}
	}

	if runStartup {
		if err := runBackfillOnce(ctx, logger, repository, appService, discordService, cfg, backfillSvc, true); err != nil {
			logger.Warn("startup backfill run failed", "error", err)
		} else {
			logNextQueuedJob(ctx, logger, repository)
		}
	}

	ticker := time.NewTicker(cfg.BackfillInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := runBackfillOnce(ctx, logger, repository, appService, discordService, cfg, backfillSvc, false); err != nil {
				logger.Warn("periodic backfill run failed", "error", err)
			}
		}
	}
}

type backfillCursorState struct {
	Active             bool      `json:"active"`
	Since              time.Time `json:"since"`
	Phase              string    `json:"phase"`
	PlaybackStartIndex int32     `json:"playback_start_index"`
	ItemsStartIndex    int32     `json:"items_start_index"`
	MaxSeen            time.Time `json:"max_seen"`
	PlaysProcessed     int       `json:"plays_processed"`
	ItemsProcessed     int       `json:"items_processed"`
}

func runBackfillOnce(ctx context.Context, logger *slog.Logger, repository repo.Repository, appService *app.Service, discordService *discord.Service, cfg config.Config, backfillSvc *jellyfin.BackfillService, isStartup bool) error {
	cursor, err := loadBackfillCursor(ctx, repository)
	if err != nil {
		return err
	}
	if !cursor.Active {
		since, err := resolveBackfillStart(ctx, repository, cfg)
		if err != nil {
			return fmt.Errorf("resolve backfill checkpoint: %w", err)
		}
		cursor = backfillCursorState{Active: true, Since: since, Phase: "items", MaxSeen: time.Now().UTC()}
		if err := saveBackfillCursor(ctx, repository, cursor); err != nil {
			return fmt.Errorf("save backfill cursor: %w", err)
		}
		logger.Info("backfill cursor initialized", "since", since, "phase", cursor.Phase)
	} else {
		if cursor.Phase == "playback" {
			cursor.Phase = "items"
			cursor.PlaybackStartIndex = 0
			if err := saveBackfillCursor(ctx, repository, cursor); err != nil {
				return fmt.Errorf("save backfill cursor: %w", err)
			}
			logger.Info("backfill cursor migrated to item-only mode", "since", cursor.Since)
		}
		logger.Info("resuming backfill from cursor", "since", cursor.Since, "phase", cursor.Phase, "playback_start_index", cursor.PlaybackStartIndex, "items_start_index", cursor.ItemsStartIndex)
	}

	startedAt := time.Now().UTC()
	logger.Info("backfill fetch started", "since", cursor.Since, "page_limit", cfg.BackfillLimit)

	for cursor.Phase == "items" {
		page, err := fetchChangedItemsPageWithRetry(ctx, logger, backfillSvc, cursor.Since, cursor.ItemsStartIndex, cfg.BackfillLimit)
		if err != nil {
			return fmt.Errorf("backfill item page fetch failed (since=%s,start=%d): %w", cursor.Since.Format(time.RFC3339), cursor.ItemsStartIndex, err)
		}
		if len(page.Items) == 0 {
			break
		}

		logger.Info("backfill item ingest page", "page_items", len(page.Items), "start_index", cursor.ItemsStartIndex)
		nextCursor := cursor
		nextCursor.ItemsProcessed += len(page.Items)
		nextCursor.MaxSeen = maxBackfillTimestamp(nextCursor.MaxSeen, computeBackfillCheckpoint(nextCursor.MaxSeen, nil, page.Items))
		if page.HasMore {
			nextCursor.ItemsStartIndex = page.NextStartIndex
		} else {
			nextCursor.Phase = "complete"
		}
		cursorJSON, err := marshalBackfillCursor(nextCursor)
		if err != nil {
			return err
		}
		if err := appService.IngestBackfillItemsWithCursor(ctx, page.Items, backfillCursorKey, cursorJSON); err != nil {
			return fmt.Errorf("item ingest: %w", err)
		}
		cursor = nextCursor
	}

	completedAt := time.Now().UTC()
	if err := saveBackfillCheckpoint(ctx, repository, completedAt); err != nil {
		logger.Error("save backfill checkpoint failed", "error", err)
	}
	if err := clearBackfillCursor(ctx, repository); err != nil {
		logger.Warn("clear backfill cursor failed", "error", err)
	}

	logger.Info("backfill completed", "since", cursor.Since, "plays_fetched", cursor.PlaysProcessed, "items_fetched", cursor.ItemsProcessed, "duration", completedAt.Sub(startedAt).String())
	if isStartup && cfg.DiscordChannelID != "" {
		msg := "Backfill complete: plays=" + strconv.Itoa(cursor.PlaysProcessed) + ", items=" + strconv.Itoa(cursor.ItemsProcessed)
		if err := discordService.SendSystemMessage(cfg.DiscordChannelID, msg); err != nil {
			logger.Warn("failed to send backfill completion announcement", "error", err)
		}
	}
	return nil
}

func retryBackoffDelay(attempt int, base time.Duration, max time.Duration) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	if attempt <= 0 {
		return base
	}

	delay := base
	for i := 0; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func ingestBackfillBatch(
	ctx context.Context,
	ingestItems func(context.Context, []jellyfin.ItemSnapshot) error,
	ingestPlayback func(context.Context, []jellyfin.PlaybackEvent) error,
	items []jellyfin.ItemSnapshot,
	plays []jellyfin.PlaybackEvent,
) error {
	if err := ingestItems(ctx, items); err != nil {
		return fmt.Errorf("item ingest: %w", err)
	}
	if err := ingestPlayback(ctx, plays); err != nil {
		return fmt.Errorf("playback ingest: %w", err)
	}
	return nil
}

func logNextQueuedJob(ctx context.Context, logger *slog.Logger, repository repo.Repository) {
	job, found, err := repository.GetNextQueuedJob(ctx)
	if err != nil {
		logger.Warn("failed to inspect next queued job", "error", err)
		return
	}
	if !found {
		logger.Info("queue state after startup reconciliation", "next_job", "none")
		return
	}

	logger.Info("queue state after startup reconciliation",
		"job_id", job.JobID,
		"kind", job.Kind,
		"item_id", job.ItemID,
		"run_at", job.RunAt,
		"status", job.Status,
	)
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
		if cfg.BackfillFullSweepOnStartup {
			return time.Time{}, nil
		}
		return now.Add(-cfg.BackfillLookback), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return now.Add(-cfg.BackfillLookback), nil
	}
	return parsed.Add(-cfg.BackfillOverlap), nil
}

func computeBackfillCheckpoint(startedAt time.Time, plays []jellyfin.PlaybackEvent, items []jellyfin.ItemSnapshot) time.Time {
	maxSeen := startedAt.UTC()
	for _, play := range plays {
		if play.Date.After(maxSeen) {
			maxSeen = play.Date
		}
	}
	for _, item := range items {
		if item.LastPlayedAt.After(maxSeen) {
			maxSeen = item.LastPlayedAt
		}
		if item.DateLastMediaAdded.After(maxSeen) {
			maxSeen = item.DateLastMediaAdded
		}
		if item.DateCreated.After(maxSeen) {
			maxSeen = item.DateCreated
		}
	}
	return maxSeen.UTC()
}

func saveBackfillCheckpoint(ctx context.Context, repository repo.Repository, at time.Time) error {
	return repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, backfillCheckpointKey, at.UTC().Format(time.RFC3339Nano))
	})
}

func fetchPlaybackPageWithRetry(ctx context.Context, logger *slog.Logger, backfillSvc *jellyfin.BackfillService, since time.Time, startIndex int32, limit int32) (jellyfin.PlaybackPage, error) {
	var lastErr error
	for attempt := 0; attempt < fetchRetryMaxAttempts; attempt++ {
		page, err := backfillSvc.FetchPlaybackEventsPage(ctx, since, startIndex, limit)
		if err == nil {
			if attempt > 0 {
				logger.Info("backfill playback fetch recovered", "attempt", attempt+1, "since", since, "start_index", startIndex)
			}
			return page, nil
		}
		lastErr = err

		delay := retryBackoffDelay(attempt, fetchRetryBaseDelay, fetchRetryMaxDelay)
		logger.Warn("backfill playback fetch retrying",
			"attempt", attempt+1,
			"max_attempts", fetchRetryMaxAttempts,
			"retry_in", delay,
			"since", since,
			"start_index", startIndex,
			"rate_limited", isRateLimitErr(err),
			"error", err,
		)

		select {
		case <-ctx.Done():
			return jellyfin.PlaybackPage{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	return jellyfin.PlaybackPage{}, lastErr
}

func fetchChangedItemsPageWithRetry(ctx context.Context, logger *slog.Logger, backfillSvc *jellyfin.BackfillService, since time.Time, startIndex int32, limit int32) (jellyfin.ItemPage, error) {
	var lastErr error
	for attempt := 0; attempt < fetchRetryMaxAttempts; attempt++ {
		page, err := backfillSvc.FetchChangedItemsPage(ctx, since, startIndex, limit)
		if err == nil {
			if attempt > 0 {
				logger.Info("backfill item fetch recovered", "attempt", attempt+1, "since", since, "start_index", startIndex)
			}
			return page, nil
		}
		lastErr = err

		delay := retryBackoffDelay(attempt, fetchRetryBaseDelay, fetchRetryMaxDelay)
		logger.Warn("backfill item fetch retrying",
			"attempt", attempt+1,
			"max_attempts", fetchRetryMaxAttempts,
			"retry_in", delay,
			"since", since,
			"start_index", startIndex,
			"rate_limited", isRateLimitErr(err),
			"error", err,
		)

		select {
		case <-ctx.Done():
			return jellyfin.ItemPage{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	return jellyfin.ItemPage{}, lastErr
}

func isRateLimitErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "429") || strings.Contains(msg, "rate limit") || strings.Contains(msg, "too many requests")
}

func loadBackfillCursor(ctx context.Context, repository repo.Repository) (backfillCursorState, error) {
	var cursor backfillCursorState
	if err := repository.WithTx(ctx, func(tx repo.TxRepository) error {
		raw, ok, err := tx.GetMeta(ctx, backfillCursorKey)
		if err != nil {
			return err
		}
		if !ok || strings.TrimSpace(raw) == "" {
			return nil
		}
		return json.Unmarshal([]byte(raw), &cursor)
	}); err != nil {
		return backfillCursorState{}, fmt.Errorf("load backfill cursor: %w", err)
	}
	return cursor, nil
}

func saveBackfillCursor(ctx context.Context, repository repo.Repository, cursor backfillCursorState) error {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return fmt.Errorf("marshal backfill cursor: %w", err)
	}
	return repository.WithTx(ctx, func(tx repo.TxRepository) error {
		return tx.SetMeta(ctx, backfillCursorKey, string(payload))
	})
}

func marshalBackfillCursor(cursor backfillCursorState) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("marshal backfill cursor: %w", err)
	}
	return string(payload), nil
}

func clearBackfillCursor(ctx context.Context, repository repo.Repository) error {
	return saveBackfillCursor(ctx, repository, backfillCursorState{})
}

func maxBackfillTimestamp(a time.Time, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
