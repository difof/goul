package binary

import "sync"

// BytePoolN returns a sync.Pool of n byte slices.
func BytePoolN(n int) sync.Pool {
	return sync.Pool{
		New: func() interface{} {
			return make([]byte, n)
		},
	}
}

// BytePool4 returns a sync.Pool of 4 byte slices.
func BytePool4() sync.Pool {
	return BytePoolN(4)
}

// BytePool8 returns a sync.Pool of 8 byte slices.
func BytePool8() sync.Pool {
	return BytePoolN(8)
}

// BytePool16 returns a sync.Pool of 16 byte slices.
func BytePool16() sync.Pool {
	return BytePoolN(16)
}

// BytePool32 returns a sync.Pool of 32 byte slices.
func BytePool32() sync.Pool {
	return BytePoolN(32)
}

// BytePool64 returns a sync.Pool of 64 byte slices.
func BytePool64() sync.Pool {
	return BytePoolN(64)
}

// BytePool128 returns a sync.Pool of 128 byte slices.
func BytePool128() sync.Pool {
	return BytePoolN(128)
}

// BytePool256 returns a sync.Pool of 256 byte slices.
func BytePool256() sync.Pool {
	return BytePoolN(256)
}
