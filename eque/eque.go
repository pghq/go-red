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
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
)

// Message is a single instance of a message within the queue.
type Message struct {
	Id  string `json:"id"`
	Value []byte `json:"value"`

	pool *redsync.Redsync
	readOptions []redsync.Option
	writeOptions []redsync.Option
	ack func() error
	reject func() error
}

// Decode fills the supplied interface with the underlying message value
func (m *Message) Decode(v interface{}) error {
	return json.Unmarshal(m.Value, v)
}

// Ack notifies upstream of successful message handling
func (m *Message) Ack(ctx context.Context) error {
	if m.ack != nil{
		if err := m.ack(); err != nil{
			return err
		}
	}

	w := m.pool.NewMutex(fmt.Sprintf("eque.w.%s", m.Id), m.writeOptions...)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.pool.NewMutex(fmt.Sprintf("eque.r.%s", m.Id), m.readOptions...)
	if _, err := r.UnlockContext(ctx); err != nil {
		return err
	}

	return nil
}

// Reject notifies upstream of unsuccessful message handling
func (m *Message) Reject(ctx context.Context) error {
	if m.reject != nil{
		if err := m.reject(); err != nil{
			return err
		}
	}

	w := m.pool.NewMutex(fmt.Sprintf("eque.w.%s", m.Id), m.writeOptions...)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.pool.NewMutex(fmt.Sprintf("eque.r.%s", m.Id), m.readOptions...)
	if _, err := r.UnlockContext(ctx); err != nil {
		return err
	}

	return nil
}

// Option is an interface for tuning a specific parameter of the queue.
type Option interface {
	Apply(conf *Config)
}

// redisOption is a queue option for configuring the underlining redis client.
type redisOption struct {
	client *redis.Client
}

func (o redisOption) Apply(conf *Config){
	if conf != nil{
		conf.options.redis = o.client
	}
}

// WithRedis creates a queue option for configuring the underlying redis client.
func WithRedis(client *redis.Client) Option{
	return redisOption{
		client: client,
	}
}

// readOptions is a queue option for configuring read locks.
type readOptions []redsync.Option

func (o readOptions) Apply(conf *Config){
	if conf != nil{
		conf.options.read = o
	}
}

// Read creates a queue option for configuring read locks.
func Read(opts ...redsync.Option) Option{
	return readOptions(opts)
}

// writeOptions is a queue option for configuring write locks.
type writeOptions []redsync.Option

func (o writeOptions) Apply(conf *Config){
	if conf != nil{
		conf.options.write = o
	}
}

// Write creates a queue option for configuring write locks.
func Write(opts ...redsync.Option) Option{
	return writeOptions(opts)
}

// consumers is a queue option for configuring the number of consumers.
type consumers int

func (o consumers) Apply(conf *Config){
	if conf != nil{
		conf.consumers = int(o)
	}
}

// WithConsumers creates a queue option for configuring the number of consumers.
func WithConsumers(count int) Option{
	return consumers(count)
}

// name is a queue option for configuring the queue name.
type name string

func (o name) Apply(conf *Config){
	if conf != nil{
		conf.name = string(o)
	}
}

// Name creates a queue option for configuring the queue name.
func Name(n string) Option{
	return name(n)
}

// interval is a queue option for configuring the queue polling interval.
type interval time.Duration

func (o interval) Apply(conf *Config){
	if conf != nil{
		conf.interval = time.Duration(o)
	}
}

// At creates a queue option for the queue polling interval.
func At(i time.Duration) Option{
	return interval(i)
}

// maxErrors is a queue option for configuring the max number of unacked errors.
type maxErrors int

func (o maxErrors) Apply(conf *Config){
	if conf != nil{
		conf.errors = int(o)
	}
}

// MaxErrors create q queue option for the maximum number of errors
func MaxErrors(limit int) Option{
	return maxErrors(limit)
}

// maxMessages is a queue option for configuring the max number of unacked messages
type maxMessages int

func (o maxMessages) Apply(conf *Config){
	if conf != nil{
		conf.messages = int(o)
	}
}

// MaxMessages create q queue option for the maximum number of messages
func MaxMessages(limit int) Option{
	return maxMessages(limit)
}
