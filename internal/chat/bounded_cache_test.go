package chat

import "testing"

// TestChatBoundedCache_EvictsLeastRecentlyUsed checks the LRU bound that keeps
// the per-file-version caches from growing without limit. Eviction must drop
// only the least recently used entry and never reorder the values of the rest.
func TestChatBoundedCache_EvictsLeastRecentlyUsed(t *testing.T) {
	cache := &chatBoundedCache{capacity: 2}

	cache.store("a", 1)
	cache.store("b", 2)

	// Touch "a" so "b" becomes the least recently used entry.
	if value, ok := cache.load("a"); !ok || value.(int) != 1 {
		t.Fatalf("load a = %v, %v want 1, true", value, ok)
	}

	cache.store("c", 3)

	if _, ok := cache.load("b"); ok {
		t.Fatal("expected b to be evicted")
	}
	if value, ok := cache.load("a"); !ok || value.(int) != 1 {
		t.Fatalf("load a after eviction = %v, %v want 1, true", value, ok)
	}
	if value, ok := cache.load("c"); !ok || value.(int) != 3 {
		t.Fatalf("load c = %v, %v want 3, true", value, ok)
	}
}

// TestChatBoundedCache_ZeroValueAndLoadOrStore checks that the zero value is
// usable (the service is often built as &ChatService{DB: db}) and that
// loadOrStore keeps the first stored value.
func TestChatBoundedCache_ZeroValueAndLoadOrStore(t *testing.T) {
	var cache chatBoundedCache

	if _, ok := cache.load("missing"); ok {
		t.Fatal("expected empty zero-value cache to miss")
	}

	first := cache.loadOrStore("k", "first")
	second := cache.loadOrStore("k", "second")
	if first != "first" || second != "first" {
		t.Fatalf("loadOrStore = %v then %v want first then first", first, second)
	}
}
