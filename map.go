package hopmap

import (
	"math/bits"
	"reflect"
)

type Hashable[K any] interface {
	Equals(K) bool
	HashCode() uint32
}

type Config struct {
	Size, BucketSize int
}

func DefaultConfig() Config {
	return Config{
		Size:       1 << 16,
		BucketSize: 32,
	}
}

type entry[K Hashable[K], V any] struct {
	key   K
	value V
}

type Map[K Hashable[K], V any] struct {
	config    Config
	entries   []*entry[K, V]
	neighbors []uint32
	size, n   int
}

func New[K Hashable[K], V any](c Config) *Map[K, V] {
	return &Map[K, V]{
		config:    c,
		entries:   make([]*entry[K, V], c.Size),
		neighbors: make([]uint32, c.Size),
		size:      c.Size,
		n:         0,
	}
}

func zeroValue[V any]() V {
	var x V
	return reflect.Zero(reflect.TypeOf(x)).Interface().(V)
}

func (m *Map[K, V]) Get(key K) (V, bool) {
	hash := m.hashKey(key)

	if e := m.findEntry(hash, key); e >= 0 {
		return m.entries[e].value, true
	}
	return zeroValue[V](), false
}

func (m *Map[K, V]) findEntry(hash uint32, key K) int {
	neighbors := m.neighbors[hash]

	zeros := bits.LeadingZeros32(neighbors)
	i := mod(int(hash)+zeros, m.size)

	for neighbors != 0 {
		if e := m.entries[i]; e.key.Equals(key) {
			return int(i)
		}

		neighbors <<= (zeros + 1)
		zeros = bits.LeadingZeros32(neighbors)
		i = mod(i+int(zeros+1), m.size)
	}
	return -1
}

func (m *Map[K, V]) hashKey(key K) uint32 {
	return key.HashCode() % uint32(m.size)
}

func (m *Map[K, V]) nextHash(hash uint32) uint32 {
	return uint32(mod(int(hash+1), m.size))
}

const (
	allBitSet = 0xFFFFFFFF
)

func mod(n, m int) int {
	res := n % m
	if res < 0 {
		return res + m
	}
	return res
}

func (m *Map[K, V]) Put(key K, value V) bool {
	hash := m.hashKey(key)

	if e := m.findEntry(hash, key); e >= 0 {
		m.entries[e].value = value
		return true
	}

	emptySlot := m.findEmptySlot(hash)
	if emptySlot < 0 || m.neighbors[emptySlot] == allBitSet {
		return false
	}

	i := int(hash)
	j, dist := m.shiftEmptySlotTo(i, emptySlot)
	if j < 0 {
		return false
	}

	m.entries[j] = &entry[K, V]{key, value}
	m.neighbors[i] |= 1 << (31 - dist)

	m.n++
	return true
}

func (m *Map[K, V]) shiftEmptySlotTo(i, j int) (int, int) {
	dist := mod(j-i, m.size)
	for dist >= int(m.config.BucketSize) {
		j = m.reshift(j)
		if j < 0 {
			return j, dist
		}
		dist = mod(j-i, m.size)
	}
	return j, dist
}

func (m *Map[K, V]) findEmptySlot(startHash uint32) int {
	if m.entries[startHash] == nil {
		return int(startHash)
	}

	hash := m.nextHash(startHash)
	for hash != startHash && m.entries[hash] != nil {
		hash = m.nextHash(hash)
	}

	if hash != startHash {
		return int(hash)
	}
	return -1
}

func (m *Map[_, _]) reshift(j int) int {
	k := m.findNearestItem(j)
	if k >= 0 {
		m.entries[j] = m.entries[k]
		m.entries[k] = nil
	}
	return k
}

// findNearestItem searches for an item whose hash value is between H-1 of j.
func (m *Map[K, V]) findNearestItem(j int) int {
	k := mod(j-1, m.size)
	maxDist := mod(j-k, m.size)
	for maxDist < m.config.BucketSize {
		if dist := bits.LeadingZeros32(m.neighbors[k]); dist <= maxDist {

			// TODO: should move this outsize
			m.clearNeighbor(k, dist)
			m.setNeighbor(k, maxDist)

			return mod(k+dist, m.size)
		}

		k = mod(k-1, m.size)
		maxDist = mod(j-k, m.size)
	}
	return -1
}

func (m *Map[_, _]) clearNeighbor(entry int, neighbor int) {
	m.neighbors[entry] ^= uint32(1 << (31 - neighbor))
}

func (m *Map[_, _]) setNeighbor(entry int, neighbor int) {
	m.neighbors[entry] |= uint32(1 << (31 - neighbor))
}

func (m *Map[K, V]) Delete(key K) (V, bool) {
	hash := m.hashKey(key)

	if e := m.findEntry(hash, key); e >= 0 {
		m.clearNeighbor(int(hash), mod(e-int(hash), m.size))

		value := m.entries[e].value
		m.resetEntry(m.entries[e])
		m.entries[e] = nil
		m.n--
		return value, true
	}
	return zeroValue[V](), false
}

func (m *Map[K, V]) resetEntry(e *entry[K, V]) {
	e.key = zeroValue[K]()
	e.value = zeroValue[V]()
}

func (m *Map[_, _]) Len() int {
	return m.n
}

func (m *Map[_, _]) Size() int {
	return m.size
}

func (m *Map[_, _]) Load() float64 {
	return float64(m.Len()) / float64(m.Size())
}
