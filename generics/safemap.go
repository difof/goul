//

package generics

import (
	"sync"
)

// SafeMap is a thread-safe map.
type SafeMap[K comparable, V any] struct {
	m    map[K]V
	lock sync.RWMutex
}

// NewSafeMap creates a new SafeMap.
func NewSafeMap[K comparable, V any](items ...Tuple[K, V]) *SafeMap[K, V] {
	var m map[K]V

	if items != nil && len(items) > 0 {
		m = make(map[K]V, len(items))
		for _, item := range items {
			m[item.Key()] = item.Value()
		}
	} else {
		m = make(map[K]V)
	}

	return &SafeMap[K, V]{m: m}
}

// GetE gets a value from the map.
func (m *SafeMap[K, V]) GetE(key K) (V, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	val, ok := m.m[key]
	return val, ok
}

// Get gets a value from the map. Panics if the key does not exist.
func (m *SafeMap[K, V]) Get(key K) V {
	v, ok := m.GetE(key)
	if !ok {
		panic("key does not exist")
	}

	return v
}

// Set sets a value in the map.
func (m *SafeMap[K, V]) Set(key K, val V) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.m[key] = val
}

// Delete deletes a value from the map.
func (m *SafeMap[K, V]) Delete(key K) {
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.m, key)
}

// Len returns the length of the map.
func (m *SafeMap[K, V]) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.m)
}

// Cap returns the capacity of the map, which is equal to the length.
func (m *SafeMap[K, V]) Cap() int {
	return m.Len()
}

// Values returns all values in the map.
func (m *SafeMap[K, V]) Values() []V {
	m.lock.RLock()
	defer m.lock.RUnlock()

	v := make([]V, 0, m.Len())
	for _, val := range m.m {
		v = append(v, val)
	}

	return v
}

// Keys returns all keys in the map.
func (m *SafeMap[K, V]) Keys() []K {
	m.lock.RLock()
	defer m.lock.RUnlock()

	k := make([]K, 0, m.Len())
	for key := range m.m {
		k = append(k, key)
	}

	return k
}

// Clear clears the map.
func (m *SafeMap[K, V]) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.m = make(map[K]V)
}

// HasKey checks if the map has the key.
func (m *SafeMap[K, V]) HasKey(key K) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	_, ok := m.m[key]
	return ok
}

// IsEmpty checks if the map is empty.
func (m *SafeMap[K, V]) IsEmpty() bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return len(m.m) == 0
}

// Iter returns a channel that iterates over the map.
func (m *SafeMap[K, V]) Iter() <-chan Tuple[K, V] {
	ch := make(chan Tuple[K, V], 1)

	go func() {
		m.lock.RLock()
		defer m.lock.RUnlock()

		for k, v := range m.m {
			ch <- NewTuple(k, v)
		}

		close(ch)
	}()

	return ch
}

// Clone returns a copy of the map.
func (m *SafeMap[K, V]) Clone() Collection[K, V, Tuple[K, V]] {
	m.lock.RLock()
	defer m.lock.RUnlock()

	items := make([]Tuple[K, V], 0, m.Len())
	for k, v := range m.m {
		items = append(items, NewTuple(k, v))
	}

	return NewSafeMap[K, V](items...)
}

func (m *SafeMap[K, V]) Collection() Collection[K, V, Tuple[K, V]] {
	return m
}
