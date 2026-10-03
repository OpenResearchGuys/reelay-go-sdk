package core

import "sync"

// BreadcrumbBuffer is a fixed-capacity ring buffer for breadcrumbs: O(1) add,
// bounded memory, no allocation churn. The most recent `capacity` crumbs are
// kept. Unlike the Node version it is safe for concurrent use, because a single
// Go request can fan out across goroutines that all share its buffer.
type BreadcrumbBuffer struct {
	mu       sync.Mutex
	items    []Breadcrumb
	capacity int
	head     int
	size     int
}

// NewBreadcrumbBuffer creates a buffer holding the latest `capacity` crumbs.
// A non-positive capacity defaults to 50 (the Node SDK default).
func NewBreadcrumbBuffer(capacity int) *BreadcrumbBuffer {
	if capacity <= 0 {
		capacity = 50
	}
	return &BreadcrumbBuffer{
		items:    make([]Breadcrumb, capacity),
		capacity: capacity,
	}
}

// Add records a crumb, overwriting the oldest when full.
func (b *BreadcrumbBuffer) Add(crumb Breadcrumb) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.items[b.head] = crumb
	b.head = (b.head + 1) % b.capacity
	if b.size < b.capacity {
		b.size++
	}
}

// Snapshot returns the crumbs oldest → newest.
func (b *BreadcrumbBuffer) Snapshot() []Breadcrumb {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return nil
	}
	out := make([]Breadcrumb, 0, b.size)
	start := (b.head - b.size + b.capacity*2) % b.capacity
	for i := 0; i < b.size; i++ {
		out = append(out, b.items[(start+i)%b.capacity])
	}
	return out
}

// Clear empties the buffer.
func (b *BreadcrumbBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head = 0
	b.size = 0
	for i := range b.items {
		b.items[i] = Breadcrumb{}
	}
}
