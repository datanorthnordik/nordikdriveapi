package chat

import (
	"container/list"
	"sync"
)

// defaultChatBoundedCacheCapacity caps how many entries a chatBoundedCache
// keeps before it evicts the least recently used one. It exists to bound the
// memory held by the per-file-version caches, which would otherwise grow
// without limit as more files and normalized versions are queried.
const defaultChatBoundedCacheCapacity = 256

// chatBoundedCache is a concurrency-safe cache with least-recently-used
// eviction. Its zero value is ready to use. Eviction only discards values that
// can be recomputed from the same pinned inputs, so it never changes an answer
// or the request flow; it only bounds resident memory.
type chatBoundedCache struct {
	mu       sync.Mutex
	capacity int
	order    *list.List
	items    map[string]*list.Element
}

type chatBoundedCacheEntry struct {
	key   string
	value any
}

func (c *chatBoundedCache) limit() int {
	if c.capacity > 0 {
		return c.capacity
	}
	return defaultChatBoundedCacheCapacity
}

func (c *chatBoundedCache) load(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items == nil {
		return nil, false
	}
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*chatBoundedCacheEntry).value, true
}

func (c *chatBoundedCache) store(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.storeLocked(key, value)
}

// loadOrStore returns the existing value for key when present, otherwise it
// stores and returns value. Concurrent callers may both compute value; the
// first stored wins, matching a plain LoadOrStore contract.
func (c *chatBoundedCache) loadOrStore(key string, value any) any {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.items != nil {
		if element, ok := c.items[key]; ok {
			c.order.MoveToFront(element)
			return element.Value.(*chatBoundedCacheEntry).value
		}
	}
	c.storeLocked(key, value)
	return value
}

func (c *chatBoundedCache) storeLocked(key string, value any) {
	if c.items == nil {
		c.items = make(map[string]*list.Element)
		c.order = list.New()
	}
	if element, ok := c.items[key]; ok {
		element.Value.(*chatBoundedCacheEntry).value = value
		c.order.MoveToFront(element)
		return
	}
	element := c.order.PushFront(&chatBoundedCacheEntry{key: key, value: value})
	c.items[key] = element
	for c.order.Len() > c.limit() {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}
		c.order.Remove(oldest)
		delete(c.items, oldest.Value.(*chatBoundedCacheEntry).key)
	}
}
