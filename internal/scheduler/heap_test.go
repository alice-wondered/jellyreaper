package scheduler

import (
	"testing"
	"time"
)

func TestWakeHeap_MinOrderAndRebalance(t *testing.T) {
	h := NewWakeHeap()
	base := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)

	h.Push(base.Add(5 * time.Minute))
	h.Push(base.Add(2 * time.Minute))
	h.Push(base.Add(7 * time.Minute))
	h.Push(base.Add(1 * time.Minute))

	peek, ok := h.Peek()
	if !ok {
		t.Fatal("expected heap to have values")
	}
	if want := base.Add(1 * time.Minute); !peek.Equal(want) {
		t.Fatalf("unexpected min peek: got=%s want=%s", peek, want)
	}

	first, _ := h.Pop()
	if want := base.Add(1 * time.Minute); !first.Equal(want) {
		t.Fatalf("unexpected first pop: got=%s want=%s", first, want)
	}

	h.Push(base.Add(30 * time.Second))
	second, _ := h.Pop()
	if want := base.Add(30 * time.Second); !second.Equal(want) {
		t.Fatalf("unexpected rebalance pop: got=%s want=%s", second, want)
	}
}

func TestWakeHeap_DiscardThrough(t *testing.T) {
	h := NewWakeHeap()
	base := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)

	h.Push(base.Add(1 * time.Minute))
	h.Push(base.Add(2 * time.Minute))
	h.Push(base.Add(3 * time.Minute))

	h.DiscardThrough(base.Add(2 * time.Minute))

	peek, ok := h.Peek()
	if !ok {
		t.Fatal("expected remaining entry")
	}
	if want := base.Add(3 * time.Minute); !peek.Equal(want) {
		t.Fatalf("unexpected remaining min: got=%s want=%s", peek, want)
	}
}
