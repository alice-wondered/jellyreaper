package scheduler

import (
	"container/heap"
	"time"
)

type timeHeap []time.Time

func (h timeHeap) Len() int { return len(h) }

func (h timeHeap) Less(i, j int) bool {
	return h[i].Before(h[j])
}

func (h timeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *timeHeap) Push(x any) {
	*h = append(*h, x.(time.Time))
}

func (h *timeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type WakeHeap struct {
	items timeHeap
}

func NewWakeHeap() *WakeHeap {
	h := &WakeHeap{items: make(timeHeap, 0)}
	heap.Init(&h.items)
	return h
}

func (h *WakeHeap) Push(at time.Time) {
	if at.IsZero() {
		return
	}
	heap.Push(&h.items, at)
}

func (h *WakeHeap) Peek() (time.Time, bool) {
	if len(h.items) == 0 {
		return time.Time{}, false
	}
	return h.items[0], true
}

func (h *WakeHeap) Pop() (time.Time, bool) {
	if len(h.items) == 0 {
		return time.Time{}, false
	}
	return heap.Pop(&h.items).(time.Time), true
}

func (h *WakeHeap) DiscardThrough(now time.Time) {
	for len(h.items) > 0 {
		next, _ := h.Peek()
		if next.After(now) {
			return
		}
		h.Pop()
	}
}
