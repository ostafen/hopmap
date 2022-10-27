package hopmap_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ostafen/hopmap"
	"github.com/stretchr/testify/require"
)

type Key uint32

func (x Key) Equals(y Key) bool {
	return x == y
}

func (x Key) HashCode() uint32 {
	return uint32(x)
}

func TestPutAndGet(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	m := hopmap.New[Key, uint32](hopmap.Config{
		Size:       1 << 12,
		BucketSize: 32,
		AutoResize: false,
	})

	keys := make([]Key, 0)
	for ok := true; ok; {
		k := rand.Int31()
		ok = m.Put(Key(k), uint32(k+1))

		if !ok {
			break
		}
		keys = append(keys, Key(k))
	}

	for _, k := range keys {
		v, ok := m.Get(k)
		require.True(t, ok)
		require.Equal(t, uint32(v), uint32(k+1))
	}
}
