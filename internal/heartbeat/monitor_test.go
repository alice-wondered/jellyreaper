package heartbeat

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	bbolt "go.etcd.io/bbolt"

	"jellyreaper/internal/repo"
	bboltrepo "jellyreaper/internal/repo/bbolt"
)

type probe struct{ err error }

func (p *probe) Ping(context.Context) error { return p.err }

type extender struct{ outages []time.Duration }

func (e *extender) ExtendDecisionDeadlines(_ context.Context, outage, _ time.Duration) (int, error) {
	e.outages = append(e.outages, outage)
	return 1, nil
}

var t0 = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func newMonitor(t *testing.T, cfg Config) (*Monitor, *probe, *extender, *time.Time) {
	t.Helper()
	store, err := bboltrepo.Open(filepath.Join(t.TempDir(), "hb.db"), 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if cfg.Grace == 0 {
		cfg.Grace = 10 * time.Minute
	}
	p, e, now := &probe{}, &extender{}, t0
	m := NewMonitor(store, p, e, cfg, nil)
	m.now = func() time.Time { return now }
	return m, p, e, &now
}

func setLastAlive(t *testing.T, m *Monitor, at time.Time) {
	t.Helper()
	if err := m.repo.WithTx(context.Background(), func(ctx context.Context, tx repo.TxRepository) error {
		return tx.SetMeta(ctx, lastAliveKey, at.Format(time.RFC3339Nano))
	}); err != nil {
		t.Fatalf("seed heartbeat: %v", err)
	}
}

func TestStartupShiftsByHeartbeatGap(t *testing.T) {
	for _, tc := range []struct {
		name      string
		lastAlive time.Time
		assumed   time.Time
		want      []time.Duration
	}{
		{"gap over grace", t0.Add(-29 * 24 * time.Hour), time.Time{}, []time.Duration{29 * 24 * time.Hour}},
		{"gap at grace", t0.Add(-10 * time.Minute), time.Time{}, []time.Duration{10 * time.Minute}},
		{"gap under grace", t0.Add(-9 * time.Minute), time.Time{}, nil},
		{"no heartbeat, assumed", time.Time{}, t0.Add(-48 * time.Hour), []time.Duration{48 * time.Hour}},
		{"heartbeat wins over assumed", t0.Add(-time.Minute), t0.Add(-48 * time.Hour), nil},
		{"no heartbeat, nothing assumed", time.Time{}, time.Time{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, e, _ := newMonitor(t, Config{AssumedLastAlive: tc.assumed})
			if !tc.lastAlive.IsZero() {
				setLastAlive(t, m, tc.lastAlive)
			}
			if err := m.Startup(context.Background()); err != nil {
				t.Fatalf("startup: %v", err)
			}
			if len(e.outages) != len(tc.want) || (len(tc.want) == 1 && e.outages[0] != tc.want[0]) {
				t.Fatalf("outages %v, want %v", e.outages, tc.want)
			}
			if got, _ := m.lastAlive(context.Background()); !got.Equal(t0) {
				t.Fatalf("startup must record a heartbeat at boot, got %v", got)
			}
		})
	}
}

func TestStartupIsIdempotentAcrossRestarts(t *testing.T) {
	m, _, e, now := newMonitor(t, Config{AssumedLastAlive: t0.Add(-48 * time.Hour)})
	_ = m.Startup(context.Background())
	*now = now.Add(time.Minute) // quick restart with ASSUME_LAST_ALIVE_AT still set
	_ = m.Startup(context.Background())
	if len(e.outages) != 1 {
		t.Fatalf("assumed last-alive must apply once, got shifts %v", e.outages)
	}
}

func TestGateNeedsJellyfinAndReconcile(t *testing.T) {
	m, p, _, _ := newMonitor(t, Config{})
	_ = m.Startup(context.Background())
	if m.DeletesAllowed() {
		t.Fatal("gate must start closed")
	}
	m.Tick(context.Background())
	if m.DeletesAllowed() {
		t.Fatal("jellyfin up but no backfill yet: must stay closed")
	}
	m.MarkReconciled()
	if !m.DeletesAllowed() {
		t.Fatal("jellyfin up and reconciled: must open")
	}
	p.err = errors.New("down")
	m.Tick(context.Background())
	if m.DeletesAllowed() {
		t.Fatal("jellyfin down: must close")
	}
}

func TestJellyfinOutageShiftsOnlyWhenSustained(t *testing.T) {
	for _, tc := range []struct {
		name   string
		outage time.Duration
		want   int
	}{
		{"blip", 3 * time.Minute, 0},
		{"sustained", 2 * time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, p, e, now := newMonitor(t, Config{})
			_ = m.Startup(context.Background())
			m.Tick(context.Background()) // up
			p.err = errors.New("down")
			*now = now.Add(time.Minute)
			m.Tick(context.Background()) // down since t0+1m
			*now = now.Add(tc.outage)
			p.err = nil
			m.Tick(context.Background())
			if len(e.outages) != tc.want || (tc.want == 1 && e.outages[0] != tc.outage) {
				t.Fatalf("outages %v, want %d of %v", e.outages, tc.want, tc.outage)
			}
		})
	}
}

func TestJellyfinDownAcrossBootMeasuredFromBoot(t *testing.T) {
	m, p, e, now := newMonitor(t, Config{})
	setLastAlive(t, m, t0.Add(-time.Minute)) // jellyreaper itself barely down
	p.err = errors.New("down")
	_ = m.Startup(context.Background())
	m.Tick(context.Background())
	*now = now.Add(3 * time.Hour)
	p.err = nil
	m.Tick(context.Background())
	if len(e.outages) != 1 || e.outages[0] != 3*time.Hour {
		t.Fatalf("want one 3h shift measured from boot, got %v", e.outages)
	}
}
