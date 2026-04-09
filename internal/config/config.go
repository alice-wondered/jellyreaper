package config

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr = ":6767"
	defaultHTTPPort = "6767"
	defaultDBPath   = "./data/jellyreaper.db"
	defaultLogDir   = "./logs"
	defaultEmbedDir = "./embeds"
	defaultLeaseTTL = 30 * time.Second
	fallbackWorker  = "jellyreaper-1"

	defaultBackfillEnabled            = true
	defaultBackfillInterval           = 15 * time.Minute
	defaultBackfillLookback           = 24 * time.Hour
	defaultBackfillOverlap            = 2 * time.Minute
	defaultBackfillLimit              = 500
	defaultBackfillFullSweep          = true
	defaultBackfillPlayback           = false
	defaultBackfillWriteBatchSize     = 100
	defaultBackfillWriteBatchTimeout  = 500 * time.Millisecond
	defaultBackfillWriteQueueCapacity = 2000
	defaultDelayWindow                = 15 * 24 * time.Hour
	defaultLastPlayedThresholdDays    = 60
	defaultHITLTimeoutHours           = 48
)

type Config struct {
	HTTPAddr string
	HTTPPort string
	DBPath   string
	LogDir   string
	EmbedDir string
	WorkerID string
	LeaseTTL time.Duration

	DiscordBotToken  string
	DiscordPublicKey []byte
	DiscordChannelID string
	OpenAIAPIKey     string
	OpenAIModel      string

	JellyfinURL          string
	JellyfinPort         string
	JellyfinAPIKey       string
	JellyfinWebhookToken string
	RadarrURL            string
	RadarrAPIKey         string
	SonarrURL            string
	SonarrAPIKey         string

	BackfillEnabled            bool
	BackfillInterval           time.Duration
	BackfillLookback           time.Duration
	BackfillOverlap            time.Duration
	BackfillLimit              int32
	BackfillFullSweepOnStartup bool
	BackfillPlaybackEnabled    bool
	BackfillWriteBatchSize     int
	BackfillWriteBatchTimeout  time.Duration
	BackfillWriteQueueCapacity int

	DefaultDelayWindow             time.Duration
	DefaultLastPlayedThresholdDays int
	DefaultHITLTimeoutHours        int
}

func LoadFromEnv() (Config, error) {
	leaseTTL := defaultLeaseTTL
	if raw := os.Getenv("LEASE_TTL"); raw != "" {
		parsed, err := time.ParseDuration(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse LEASE_TTL: %w", err)
		}
		if parsed <= 0 {
			return Config{}, fmt.Errorf("parse LEASE_TTL: must be > 0")
		}
		leaseTTL = parsed
	}

	httpPort := envOrDefault("HTTP_PORT", defaultHTTPPort)
	httpAddr := envOrDefault("HTTP_ADDR", "")
	if httpAddr == "" {
		httpAddr = ":" + httpPort
	}

	jellyfinURL := envOrDefault("JELLYFIN_URL", "")
	jellyfinPort := envOrDefault("JELLYFIN_PORT", "8096")
	if jellyfinURL == "" {
		host := envOrDefault("JELLYFIN_HOST", "localhost")
		jellyfinURL = "http://" + host + ":" + jellyfinPort
	}

	backfillInterval, err := parseDurationEnv("BACKFILL_INTERVAL", defaultBackfillInterval)
	if err != nil {
		return Config{}, err
	}
	backfillLookback, err := parseDurationEnv("BACKFILL_LOOKBACK", defaultBackfillLookback)
	if err != nil {
		return Config{}, err
	}
	backfillOverlap, err := parseDurationEnv("BACKFILL_OVERLAP", defaultBackfillOverlap)
	if err != nil {
		return Config{}, err
	}

	backfillLimit := int32(defaultBackfillLimit)
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_LIMIT")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("parse BACKFILL_LIMIT: must be a positive integer")
		}
		backfillLimit = int32(parsed)
	}

	backfillEnabled := defaultBackfillEnabled
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_ENABLED")); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse BACKFILL_ENABLED: %w", err)
		}
		backfillEnabled = parsed
	}

	backfillFullSweep := defaultBackfillFullSweep
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_FULL_SWEEP_ON_STARTUP")); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse BACKFILL_FULL_SWEEP_ON_STARTUP: %w", err)
		}
		backfillFullSweep = parsed
	}

	backfillPlaybackEnabled := defaultBackfillPlayback
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_PLAYBACK_ENABLED")); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse BACKFILL_PLAYBACK_ENABLED: %w", err)
		}
		backfillPlaybackEnabled = parsed
	}

	backfillWriteBatchSize := defaultBackfillWriteBatchSize
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_WRITE_BATCH_SIZE")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("parse BACKFILL_WRITE_BATCH_SIZE: must be a positive integer")
		}
		backfillWriteBatchSize = parsed
	}

	backfillWriteBatchTimeout, err := parseDurationEnv("BACKFILL_WRITE_BATCH_TIMEOUT", defaultBackfillWriteBatchTimeout)
	if err != nil {
		return Config{}, err
	}

	backfillWriteQueueCapacity := defaultBackfillWriteQueueCapacity
	if raw := strings.TrimSpace(os.Getenv("BACKFILL_WRITE_QUEUE_CAPACITY")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("parse BACKFILL_WRITE_QUEUE_CAPACITY: must be a positive integer")
		}
		backfillWriteQueueCapacity = parsed
	}

	defaultDelayWindow, err := parseDurationEnv("DEFAULT_DELAY_WINDOW", defaultDelayWindow)
	if err != nil {
		return Config{}, err
	}

	defaultLastPlayedThresholdDays := defaultLastPlayedThresholdDays
	if raw := strings.TrimSpace(os.Getenv("DEFAULT_LAST_PLAYED_THRESHOLD_DAYS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("parse DEFAULT_LAST_PLAYED_THRESHOLD_DAYS: must be a positive integer")
		}
		defaultLastPlayedThresholdDays = parsed
	}

	defaultHITLTimeoutHours := defaultHITLTimeoutHours
	if raw := strings.TrimSpace(os.Getenv("DEFAULT_HITL_TIMEOUT_HOURS")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return Config{}, fmt.Errorf("parse DEFAULT_HITL_TIMEOUT_HOURS: must be a positive integer")
		}
		defaultHITLTimeoutHours = parsed
	}

	cfg := Config{
		HTTPAddr: httpAddr,
		HTTPPort: httpPort,
		DBPath:   envOrDefault("DB_PATH", defaultDBPath),
		LogDir:   envOrDefault("LOG_DIR", defaultLogDir),
		EmbedDir: envOrDefault("EMBED_PERSIST_DIR", defaultEmbedDir),
		WorkerID: resolveWorkerID(),
		LeaseTTL: leaseTTL,

		DiscordBotToken:      os.Getenv("DISCORD_BOT_TOKEN"),
		DiscordChannelID:     os.Getenv("DISCORD_CHANNEL_ID"),
		OpenAIAPIKey:         strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		OpenAIModel:          envOrDefault("OPENAI_MODEL", "gpt-4o-mini"),
		JellyfinURL:          jellyfinURL,
		JellyfinPort:         jellyfinPort,
		JellyfinAPIKey:       os.Getenv("JELLYFIN_API_KEY"),
		JellyfinWebhookToken: strings.TrimSpace(os.Getenv("JELLYFIN_WEBHOOK_TOKEN")),
		RadarrURL:            strings.TrimSpace(os.Getenv("RADARR_URL")),
		RadarrAPIKey:         strings.TrimSpace(os.Getenv("RADARR_API_KEY")),
		SonarrURL:            strings.TrimSpace(os.Getenv("SONARR_URL")),
		SonarrAPIKey:         strings.TrimSpace(os.Getenv("SONARR_API_KEY")),

		BackfillEnabled:            backfillEnabled,
		BackfillInterval:           backfillInterval,
		BackfillLookback:           backfillLookback,
		BackfillOverlap:            backfillOverlap,
		BackfillLimit:              backfillLimit,
		BackfillFullSweepOnStartup: backfillFullSweep,
		BackfillPlaybackEnabled:    backfillPlaybackEnabled,
		BackfillWriteBatchSize:     backfillWriteBatchSize,
		BackfillWriteBatchTimeout:  backfillWriteBatchTimeout,
		BackfillWriteQueueCapacity: backfillWriteQueueCapacity,

		DefaultDelayWindow:             defaultDelayWindow,
		DefaultLastPlayedThresholdDays: defaultLastPlayedThresholdDays,
		DefaultHITLTimeoutHours:        defaultHITLTimeoutHours,
	}

	if raw := os.Getenv("DISCORD_PUBLIC_KEY_HEX"); raw != "" {
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			return Config{}, fmt.Errorf("parse DISCORD_PUBLIC_KEY_HEX: %w", err)
		}
		cfg.DiscordPublicKey = decoded
	}

	return cfg, nil
}

func parseDurationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	if strings.HasSuffix(value, "d") || strings.HasSuffix(value, "D") {
		rawDays := strings.TrimSpace(value[:len(value)-1])
		days, err := strconv.Atoi(rawDays)
		if err != nil || days <= 0 {
			return 0, fmt.Errorf("parse %s: invalid day duration", key)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", key, err)
	}
	if parsed <= 0 {
		return 0, fmt.Errorf("parse %s: must be > 0", key)
	}
	return parsed, nil
}

func resolveWorkerID() string {
	if workerID := os.Getenv("WORKER_ID"); workerID != "" {
		return workerID
	}
	hostname, err := os.Hostname()
	if err == nil && hostname != "" {
		return hostname
	}
	return fallbackWorker
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
