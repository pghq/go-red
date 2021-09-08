package eque

import (
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
)

// QueueOption is an interface for tuning a specific parameter of the queue.
type QueueOption interface {
	Apply(conf *QueueConfig)
}

// redisOptions is a queue option for configuring the underlining redis client.
type redisOptions redis.Options

func (o redisOptions) Apply(conf *QueueConfig){
	if conf != nil{
		conf.redisOptions = redis.Options(o)
	}
}

// WithRedis creates a queue option for configuring the underlying redis client.
func WithRedis(opts redis.Options) QueueOption{
	return redisOptions(opts)
}

// rLockOptions is a queue option for configuring read locks.
type rLockOptions []redsync.Option

func (o rLockOptions) Apply(conf *QueueConfig){
	if conf != nil{
		conf.rLockOptions = o
	}
}

// WithRLock creates a queue option for configuring read locks.
func WithRLock(opts []redsync.Option) QueueOption{
	return rLockOptions(opts)
}

// wLockOptions is a queue option for configuring write locks.
type wLockOptions []redsync.Option

func (o wLockOptions) Apply(conf *QueueConfig){
	if conf != nil{
		conf.wLockOptions = o
	}
}

// WithWLock creates a queue option for configuring write locks.
func WithWLock(opts []redsync.Option) QueueOption{
	return wLockOptions(opts)
}

// emitBackgroundError is a queue option for configuring background error handling.
type emitBackgroundError func(err error)

func (o emitBackgroundError) Apply(conf *QueueConfig){
	if conf != nil{
		conf.emitBackgroundError = o
	}
}

// WithEmitBackgroundError creates a queue option for emitting background errors.
func WithEmitBackgroundError(emit func(err error)) QueueOption{
	return emitBackgroundError(emit)
}


