package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/cache/v8"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

type Connection struct {
	Client *redis.Client
	Cache  *cache.Cache
}

// Connect to redis via config
func Connect(opts *Options) (ctx Connection, err error) {
	cl := redis.NewClient(&redis.Options{
		Addr: opts.Addr,
		OnConnect: func(ctx context.Context, cn *redis.Conn) error {
			log.Printf("connected to redis")
			return nil
		},
		Username:     opts.Username,
		Password:     opts.Password,
		DB:           opts.DB,
		MaxRetries:   opts.MaxRetries,
		DialTimeout:  opts.DialTimeout,
		ReadTimeout:  opts.ReadTimeout,
		WriteTimeout: opts.WriteTimeout,
		PoolTimeout:  opts.PoolTimeout,
		IdleTimeout:  opts.IdleTimeout,
		PoolFIFO:     false,
		TLSConfig:    opts.TLSConfig,
		//IdleCheckFrequency: 0,
		//MinIdleConns:       0,
	})

	ctx = Connection{
		Client: cl,
		Cache: cache.New(&cache.Options{
			Redis: cl,
			//LocalCache: cache.NewTinyLFU(1000, time.Minute),
		}),
	}

	return
}

// Close closes the redis connection
func (conn Connection) Close() error {
	return conn.Client.Close()
}

// GetCache gets a redis item
func (conn Connection) GetCache(key string, val interface{}) error {
	return conn.GetCacheContext(context.Background(), key, val)
}

// GetCacheContext gets a redis item with context
func (conn Connection) GetCacheContext(c context.Context, key string, val interface{}) error {
	if err := conn.Cache.Get(c, key, val); err != nil {
		return fmt.Errorf("redis get error: %w", err)
	}

	return nil
}

// SetCache sets a redis with one hour TTL
func (conn Connection) SetCache(key string, val interface{}, override bool) error {
	return conn.SetCacheTTLContext(context.Background(), key, val, override, 0)
}

// SetCacheContext sets a redis with one hour TTL with context
func (conn Connection) SetCacheContext(c context.Context, key string, val interface{}, override bool) error {
	return conn.SetCacheTTLContext(c, key, val, override, 0)
}

// SetCacheTTLContext sets a redis with a TTL
func (conn Connection) SetCacheTTLContext(c context.Context, key string, val interface{}, override bool, ttl time.Duration) error {
	if err := conn.Cache.Set(&cache.Item{
		Ctx:   c,
		Key:   key,
		Value: val,
		TTL:   ttl,
		SetXX: override,
		SetNX: !override,
	}); err != nil {
		return fmt.Errorf("redis set error: %w", err)
	}

	return nil
}

// DeleteCache deletes a redis item
func (conn Connection) DeleteCache(key string) error {
	return conn.DeleteCacheContext(context.Background(), key)
}

// DeleteCacheContext deletes a redis item with context
func (conn Connection) DeleteCacheContext(c context.Context, key string) error {
	if err := conn.Cache.Delete(c, key); err != nil {
		return fmt.Errorf("redis delete error: %w", err)
	}

	return nil
}

// Exists checks if a key exists
func (conn Connection) Exists(key string) bool {
	return conn.ExistsContext(context.Background(), key)
}

// ExistsContext checks if a key exists with context
func (conn Connection) ExistsContext(c context.Context, key string) bool {
	return conn.Cache.Exists(c, key)
}
