package heartbeat

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"jellyreaper/internal/repo"
)

const lastAliveKey = "heartbeat.last_alive_at"

type Prober interface {
	Ping(context.Context) error
}

type DeadlineExtender interface {
	ExtendDecisionDeadlines(ctx context.Context, outage, minWindow time.Duration) (int, error)
}

type Config struct {
	Interval  time.Duration // liveness write + Jellyfin probe cadence
	Grace     time.Duration // outages shorter than this shift nothing
	MinWindow time.Duration // floor on a pending decision's remaining window after a shift
	// AssumedLastAlive seeds the boot gap when no heartbeat was ever
	// recorded (first boot of a heartbeat-aware build). Ignored afterwards,
	// so leaving it set across restarts never double-shifts.
	AssumedLastAlive time.Time
}

// Monitor answers one question for the dispatcher: may unattended deletes
// run right now? Only when Jellyfin answers AND a backfill has reconciled
// plays since boot. Outages (jellyreaper's own, measured from the persisted
// heartbeat, and Jellyfin's, measured in-process) extend pending HITL
// deadlines so time nobody could watch or click never counts against an item.
type Monitor struct {
	repo     repo.Repository
	probe    Prober
	deadline DeadlineExtender
	cfg      Config
	logger   *slog.Logger
	now      func() time.Time

	mu         sync.Mutex
	jellyfinUp bool
	downSince  time.Time
	reconciled bool
}

func NewMonitor(repository repo.Repository, probe Prober, deadline DeadlineExtender, cfg Config, logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Monitor{repo: repository, probe: probe, deadline: deadline, cfg: cfg, logger: logger, now: func() time.Time { return time.Now().UTC() }}
}

func (m *Monitor) DeletesAllowed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jellyfinUp && m.reconciled
}

// MarkReconciled opens the reconcile half of the gate. Call after a backfill
// succeeds, so plays from any downtime are ingested before a timeout acts.
func (m *Monitor) MarkReconciled() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.reconciled {
		m.logger.Info("deletes unblocked by successful backfill", "lex", "HEARTBEAT")
	}
	m.reconciled = true
}

// Startup must run before the scheduler starts. Jellyfin is treated as down
// from boot until the first successful probe, so a Jellyfin outage that
// spans the restart is measured from here.
func (m *Monitor) Startup(ctx context.Context) error {
	now := m.now()
	lastAlive, err := m.lastAlive(ctx)
	if err != nil {
		return err
	}
	source := "heartbeat"
	if lastAlive.IsZero() {
		lastAlive, source = m.cfg.AssumedLastAlive, "assumed"
	}
	if gap := now.Sub(lastAlive); !lastAlive.IsZero() && gap >= m.cfg.Grace {
		if err := m.extend(ctx, gap, "jellyreaper_down", "last_alive_source", source, "last_alive", lastAlive); err != nil {
			return err
		}
	} else if lastAlive.IsZero() {
		m.logger.Warn("no heartbeat recorded and no assumed last-alive; boot outage not measured", "lex", "HEARTBEAT")
	}
	m.mu.Lock()
	m.downSince = now
	m.mu.Unlock()
	return m.beat(ctx, now)
}

func (m *Monitor) Run(ctx context.Context) {
	m.Tick(ctx)
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.Tick(ctx)
		}
	}
}

func (m *Monitor) Tick(ctx context.Context) {
	now := m.now()
	if err := m.beat(ctx, now); err != nil {
		m.logger.Warn("heartbeat write failed", "lex", "HEARTBEAT", "error", err)
	}
	probeErr := m.probe.Ping(ctx)

	m.mu.Lock()
	wasUp, downSince := m.jellyfinUp, m.downSince
	m.jellyfinUp = probeErr == nil
	if probeErr != nil && wasUp {
		m.downSince = now
	}
	m.mu.Unlock()

	switch {
	case probeErr != nil && wasUp:
		m.logger.Warn("jellyfin unreachable; deletes paused", "lex", "HEARTBEAT", "error", probeErr)
	case probeErr == nil && !wasUp:
		outage := now.Sub(downSince)
		m.logger.Info("jellyfin reachable", "lex", "HEARTBEAT", "outage", outage.String())
		if outage >= m.cfg.Grace {
			if err := m.extend(ctx, outage, "jellyfin_down", "down_since", downSince); err != nil {
				m.logger.Error("deadline extension failed", "lex", "HEARTBEAT", "error", err)
			}
		}
	}
}

func (m *Monitor) extend(ctx context.Context, outage time.Duration, reason string, fields ...any) error {
	n, err := m.deadline.ExtendDecisionDeadlines(ctx, outage, m.cfg.MinWindow)
	if err != nil {
		return fmt.Errorf("extend decision deadlines after %s: %w", reason, err)
	}
	m.logger.Info("outage shifted pending decision deadlines", append([]any{"lex", "HEARTBEAT", "reason", reason, "outage", outage.String(), "flows_extended", n}, fields...)...)
	return nil
}

func (m *Monitor) lastAlive(ctx context.Context) (time.Time, error) {
	var raw string
	var ok bool
	if err := m.repo.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		var err error
		raw, ok, err = tx.GetMeta(ctx, lastAliveKey)
		return err
	}); err != nil || !ok {
		return time.Time{}, err
	}
	at, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %s %q: %w", lastAliveKey, raw, err)
	}
	return at, nil
}

func (m *Monitor) beat(ctx context.Context, at time.Time) error {
	return m.repo.WithTx(ctx, func(ctx context.Context, tx repo.TxRepository) error {
		return tx.SetMeta(ctx, lastAliveKey, at.UTC().Format(time.RFC3339Nano))
	})
}
