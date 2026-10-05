package lru

import (
	"strconv"
	"sync"
	"testing"
)

func TestBoundedAtCapacity(t *testing.T) {
	for _, capacity := range []int{1, 2, 3} {
		c := New[string, int](capacity)
		for i := 0; i < capacity+5; i++ {
			c.Put(strconv.Itoa(i), i)
			if c.Len() > capacity {
				t.Fatalf("capacity %d: len %d after %d puts", capacity, c.Len(), i+1)
			}
		}
	}
	if New[string, int](0).capacity != 1 {
		t.Fatal("non-positive capacity must clamp to 1")
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	c := New[string, int](2)
	c.Put("a", 1)
	c.Put("b", 2)
	c.Get("a") // b is now least recent
	c.Put("c", 3)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatal("a should survive")
	}
	c.Put("a", 10) // update in place, no growth
	if v, _ := c.Get("a"); v != 10 || c.Len() != 2 {
		t.Fatalf("update: v=%d len=%d", v, c.Len())
	}
}

func TestAddOnlyWhenAbsent(t *testing.T) {
	c := New[string, struct{}](4)
	if !c.Add("x", struct{}{}) || c.Add("x", struct{}{}) {
		t.Fatal("Add must report first insertion only")
	}
}

func TestConcurrentUseStaysBounded(t *testing.T) {
	c := New[int, int](64)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				c.Put(g*1000+i, i)
				c.Get(i)
			}
		}(g)
	}
	wg.Wait()
	if c.Len() != 64 {
		t.Fatalf("len %d, want 64", c.Len())
	}
}
