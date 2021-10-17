// Copyright 2021 PGHQ. All Rights Reserved.
//
// Licensed under the GNU General Public License, Version 3 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package eque provides resources for interacting with this application.
//
// Any functionality provided by this package should not
// depend on any external packages within the application.
package eque

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

const (
	// DefaultConsumers is the total number of consumers processing messages
	DefaultConsumers = 10

	// DefaultName is the default queue name
	DefaultName = "eque.messages"

	// DefaultInterval is the duration the queue sleeps before checking for new deliveries
	DefaultInterval = 100 * time.Millisecond
)

// eque is an instance of the exclusive message queue.
type eque struct {
	queue rmq.Queue
	messages chan *message
	locks locks
	backgroundErrors chan error
	emitBackgroundError func(err error)
}

// NewRedQueue creates an instance of the exclusive queue
func NewRedQueue(addr string, opts... QueueOption) (RedQueue, error) {
	conf := QueueConfig{
		name: DefaultName,
		consumers: DefaultConsumers,
		interval: DefaultInterval,
	}
	for _, opt := range opts{
		opt.Apply(&conf)
	}

	conf.redisOptions.Addr = addr

	var backgroundErrors chan error
	if conf.emitBackgroundError != nil{
		backgroundErrors = make(chan error)
	}

	client := redis.NewClient(&conf.redisOptions)
	conn, err := rmq.OpenConnectionWithRedisClient(conf.name, client, backgroundErrors)
	if err != nil {
		return nil, err
	}

	mq, err := conn.OpenQueue(conf.name)
	if err != nil {
		return nil, err
	}

	if err := mq.StartConsuming(int64(conf.consumers + 1), conf.interval); err != nil {
		return nil, err
	}

	pool := goredis.NewPool(client)
	queue := eque{
		queue: mq,
		messages: make(chan *message),
		locks: locks{
			rs:    redsync.New(pool),
			readOpts: conf.rLockOptions,
			writeOpts: conf.wLockOptions,
		},
	}

	for i := 0; i < conf.consumers; i++ {
		tag := fmt.Sprintf("eque.consumner.%d", i + 1)
		_, err = mq.AddConsumerFunc(tag, func(delivery rmq.Delivery) {
			var msg message
			if err := json.Unmarshal([]byte(delivery.Payload()), &msg); err != nil {
				queue.EmitBackgroundError(err)
				if err := delivery.Reject(); err != nil {
					queue.EmitBackgroundError(err)
				}
				return
			}

			msg.ack = delivery.Ack
			msg.reject = delivery.Reject
			msg.locks = queue.locks
			queue.messages <- &msg
		})

		if err != nil {
			return nil, err
		}
	}

	if queue.emitBackgroundError != nil{
		go func() {
			for err := range backgroundErrors {
				queue.emitBackgroundError(err)
			}
		}()
	}

	return &queue, nil
}

// EmitBackgroundError emits background errors if any to the queue consumer
func (q *eque) EmitBackgroundError(err error){
	if q.backgroundErrors == nil{
		return
	}

	q.backgroundErrors <- err
}

// QueueConfig is a configuration object providing options for tuning the queue.
type QueueConfig struct{
	redisOptions redis.Options
	rLockOptions []redsync.Option
	wLockOptions []redsync.Option
	emitBackgroundError func(err error)
	consumers int
	name string
	interval time.Duration
}

// locks is a shared service for the queue and messages for obtaining locks
type locks struct{
	rs    *redsync.Redsync
	readOpts []redsync.Option
	writeOpts []redsync.Option
}

// NewRMutex creates a new mutex for queue read operations
func (l *locks) NewRMutex(name string) *redsync.Mutex{
	return l.rs.NewMutex(fmt.Sprintf("eque.rlocks.%s", name), l.readOpts...)
}

// NewWMutex creates a new mutex for queue write operations
func (l *locks) NewWMutex(name string) *redsync.Mutex{
	return l.rs.NewMutex(fmt.Sprintf("eque.wlocks.%s", name), l.writeOpts...)
}

