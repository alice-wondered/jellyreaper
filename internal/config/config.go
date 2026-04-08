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

	defaultBackfillEnabled  = true
	defaultBackfillInterval = 15 * time.Minute
	defaultBackfillLookback = 24 * time.Hour
	defaultBackfillOverlap  = 2 * time.Minute
	defaultBackfillLimit    = 500
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

	JellyfinURL    string
	JellyfinPort   string
	JellyfinAPIKey string

	BackfillEnabled  bool
	BackfillInterval time.Duration
	BackfillLookback time.Duration
	BackfillOverlap  time.Duration
	BackfillLimit    int32
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

	cfg := Config{
		HTTPAddr: httpAddr,
		HTTPPort: httpPort,
		DBPath:   envOrDefault("DB_PATH", defaultDBPath),
		LogDir:   envOrDefault("LOG_DIR", defaultLogDir),
		EmbedDir: envOrDefault("EMBED_PERSIST_DIR", defaultEmbedDir),
		WorkerID: resolveWorkerID(),
		LeaseTTL: leaseTTL,

		DiscordBotToken:  os.Getenv("DISCORD_BOT_TOKEN"),
		DiscordChannelID: os.Getenv("DISCORD_CHANNEL_ID"),
		JellyfinURL:      jellyfinURL,
		JellyfinPort:     jellyfinPort,
		JellyfinAPIKey:   os.Getenv("JELLYFIN_API_KEY"),

		BackfillEnabled:  backfillEnabled,
		BackfillInterval: backfillInterval,
		BackfillLookback: backfillLookback,
		BackfillOverlap:  backfillOverlap,
		BackfillLimit:    backfillLimit,
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
