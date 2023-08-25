package redis

import (
	"context"
	"crypto/tls"
	"github.com/go-redis/redis/v8"
	"time"
)

type Options struct {
	Addr            string
	Username        string
	Password        string
	DB              int
	MaxRetries      int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
	IdleTimeout     time.Duration
	PoolFIFO        bool
	TLSConfig       *tls.Config
	ConnectCallback func(ctx context.Context, cn *redis.Conn) error
}

func NewOptions() *Options {
	return &Options{}
}

func (o *Options) SetAddr(addr string) *Options {
	o.Addr = addr
	return o
}

func (o *Options) SetUsername(username string) *Options {
	o.Username = username
	return o
}

func (o *Options) SetPassword(password string) *Options {
	o.Password = password
	return o
}

func (o *Options) SetDB(db int) *Options {
	o.DB = db
	return o
}

func (o *Options) SetMaxRetries(maxRetries int) *Options {
	o.MaxRetries = maxRetries
	return o
}

func (o *Options) SetDialTimeout(dialTimeout time.Duration) *Options {
	o.DialTimeout = dialTimeout
	return o
}

func (o *Options) SetReadTimeout(readTimeout time.Duration) *Options {
	o.ReadTimeout = readTimeout
	return o
}

func (o *Options) SetWriteTimeout(writeTimeout time.Duration) *Options {
	o.WriteTimeout = writeTimeout
	return o
}

func (o *Options) SetPoolTimeout(poolTimeout time.Duration) *Options {
	o.PoolTimeout = poolTimeout
	return o
}

func (o *Options) SetIdleTimeout(idleTimeout time.Duration) *Options {
	o.IdleTimeout = idleTimeout
	return o
}

func (o *Options) SetPoolFIFO(poolFIFO bool) *Options {
	o.PoolFIFO = poolFIFO
	return o
}

func (o *Options) SetTLSConfig(tlsConfig *tls.Config) *Options {
	o.TLSConfig = tlsConfig
	return o
}

func (o *Options) SetConnectCallback(connectCallback func(ctx context.Context, cn *redis.Conn) error) *Options {
	o.ConnectCallback = connectCallback
	return o
}
