// Package lru is a fixed-capacity, concurrency-safe least-recently-used map.
package lru

import (
	"container/list"
	"sync"
)

// Cache evicts the least recently used entry once Len reaches capacity, so
// memory is O(capacity) regardless of library size. Get and Put are O(1).
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	capacity int
	order    *list.List // front = most recently used
	entries  map[K]*list.Element
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

func New[K comparable, V any](capacity int) *Cache[K, V] {
	if capacity < 1 {
		capacity = 1
	}
	return &Cache[K, V]{capacity: capacity, order: list.New(), entries: make(map[K]*list.Element, capacity)}
}

func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.entries[key]
	if !ok {
		var zero V
		return zero, false
	}
	c.order.MoveToFront(el)
	return el.Value.(*entry[K, V]).value, true
}

func (c *Cache[K, V]) Put(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.entries[key]; ok {
		el.Value.(*entry[K, V]).value = value
		c.order.MoveToFront(el)
		return
	}
	c.entries[key] = c.order.PushFront(&entry[K, V]{key: key, value: value})
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*entry[K, V]).key)
	}
}

// Add stores key only if absent and reports whether it was added.
func (c *Cache[K, V]) Add(key K, value V) bool {
	if _, ok := c.Get(key); ok {
		return false
	}
	c.Put(key, value)
	return true
}

func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}
