package red

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/pghq/go-tea"
)

const (
	// DefaultConsumers is the total number of consumers processing messages
	DefaultConsumers = 10

	// DefaultName is the default queue name
	DefaultName = "red.messages"

	// DefaultQueueInterval is the duration the queue sleeps before checking for new deliveries
	DefaultQueueInterval = 100 * time.Millisecond

	// DefaultMaxMessages is the default max number of messages to keep before dropping occurs
	DefaultMaxMessages = 1024

	// DefaultMaxErrors is the default max number of errors to track before dropping occurs
	DefaultMaxErrors = 1024
)

// Red is an instance of the exclusive message queue.
type Red struct {
	queue        rmq.Queue
	messages     chan *Message
	errors       chan error
	errorSize    int
	messageSize  int
	pool         *redsync.Redsync
	readOptions  []redsync.Option
	writeOptions []redsync.Option
}

// Send batches messages
func (r *Red) Send(msg *Message) *Red {
	select {
	case r.messages <- msg:
	default:
	}

	return r
}

// Message pops a message from the queue
func (r *Red) Message() *Message {
	select {
	case msg := <-r.messages:
		return msg
	default:
		return nil
	}
}

// SendError batches errors
func (r *Red) SendError(err error) *Red {
	select {
	case r.errors <- err:
	default:
	}

	return r
}

// Error checks if any errors have occurred
func (r *Red) Error() error {
	select {
	case err := <-r.errors:
		return err
	default:
		return nil
	}
}

// consume is handles rmq deliveries
func (r *Red) consume(delivery rmq.Delivery) {
	var msg Message
	if err := json.Unmarshal([]byte(delivery.Payload()), &msg); err != nil {
		r.SendError(tea.Error(err))
		return
	}

	msg.ack = delivery.Ack
	msg.reject = delivery.Reject
	msg.pool = r.pool
	msg.readOptions = r.readOptions
	msg.writeOptions = r.writeOptions
	r.Send(&msg)
}

// NewQueue creates an instance of the exclusive queue
func NewQueue(addr string, opts ...Option) (*Red, error) {
	conf := Config{
		name:      DefaultName,
		consumers: DefaultConsumers,
		interval:  DefaultQueueInterval,
		messages:  DefaultMaxMessages,
		errors:    DefaultMaxErrors,
	}

	for _, opt := range opts {
		opt.Apply(&conf)
	}

	client := conf.options.redis
	if client == nil {
		client = redis.NewClient(&redis.Options{
			Addr: addr,
		})
	}

	pool := goredis.NewPool(client)
	q := Red{
		messages:     make(chan *Message, conf.messages),
		errors:       make(chan error, conf.errors),
		pool:         redsync.New(pool),
		readOptions:  conf.options.read,
		writeOptions: conf.options.write,
	}

	conn, err := rmq.OpenConnectionWithRedisClient(conf.name, client, q.errors)
	if err != nil {
		return nil, tea.Error(err)
	}

	mq, err := conn.OpenQueue(conf.name)
	if err != nil {
		return nil, tea.Error(err)
	}

	if err := mq.StartConsuming(int64(conf.consumers+1), conf.interval); err != nil {
		return nil, tea.Error(err)
	}

	q.queue = mq
	for i := 0; i < conf.consumers; i++ {
		tag := fmt.Sprintf("red.consumer.%d", i+1)
		_, err = mq.AddConsumerFunc(tag, q.consume)

		if err != nil {
			return nil, tea.Error(err)
		}
	}

	return &q, nil
}

// Message is a single instance of a message within the queue.
type Message struct {
	Id    string `json:"id"`
	Value []byte `json:"value"`

	pool         *redsync.Redsync
	readOptions  []redsync.Option
	writeOptions []redsync.Option
	ack          func() error
	reject       func() error
}

// Decode fills the supplied interface with the underlying message value
func (m *Message) Decode(v interface{}) error {
	return json.Unmarshal(m.Value, v)
}

// Ack notifies upstream of successful message handling
func (m *Message) Ack(ctx context.Context) error {
	if m.ack != nil {
		if err := m.ack(); err != nil {
			return err
		}
	}

	w := m.pool.NewMutex(fmt.Sprintf("red.w.%s", m.Id), m.writeOptions...)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.pool.NewMutex(fmt.Sprintf("red.r.%s", m.Id), m.readOptions...)
	if _, err := r.UnlockContext(ctx); err != nil {
		return err
	}

	return nil
}

// Reject notifies upstream of unsuccessful message handling
func (m *Message) Reject(ctx context.Context) error {
	if m.reject != nil {
		if err := m.reject(); err != nil {
			return err
		}
	}

	w := m.pool.NewMutex(fmt.Sprintf("red.w.%s", m.Id), m.writeOptions...)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.pool.NewMutex(fmt.Sprintf("red.r.%s", m.Id), m.readOptions...)
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

func (o redisOption) Apply(conf *Config) {
	if conf != nil {
		conf.options.redis = o.client
	}
}

// WithRedis creates a queue option for configuring the underlying redis client.
func WithRedis(client *redis.Client) Option {
	return redisOption{
		client: client,
	}
}

// readOptions is a queue option for configuring read locks.
type readOptions []redsync.Option

func (o readOptions) Apply(conf *Config) {
	if conf != nil {
		conf.options.read = o
	}
}

// Read creates a queue option for configuring read locks.
func Read(opts ...redsync.Option) Option {
	return readOptions(opts)
}

// writeOptions is a queue option for configuring write locks.
type writeOptions []redsync.Option

func (o writeOptions) Apply(conf *Config) {
	if conf != nil {
		conf.options.write = o
	}
}

// Write creates a queue option for configuring write locks.
func Write(opts ...redsync.Option) Option {
	return writeOptions(opts)
}

// consumers is a queue option for configuring the number of consumers.
type consumers int

func (o consumers) Apply(conf *Config) {
	if conf != nil {
		conf.consumers = int(o)
	}
}

// WithConsumers creates a queue option for configuring the number of consumers.
func WithConsumers(count int) Option {
	return consumers(count)
}

// name is a queue option for configuring the queue name.
type name string

func (o name) Apply(conf *Config) {
	if conf != nil {
		conf.name = string(o)
	}
}

// Name creates a queue option for configuring the queue name.
func Name(n string) Option {
	return name(n)
}

// interval is a queue option for configuring the queue polling interval.
type interval time.Duration

func (o interval) Apply(conf *Config) {
	if conf != nil {
		conf.interval = time.Duration(o)
	}
}

// At creates a queue option for the queue polling interval.
func At(i time.Duration) Option {
	return interval(i)
}

// maxErrors is a queue option for configuring the max number of unacked errors.
type maxErrors int

func (o maxErrors) Apply(conf *Config) {
	if conf != nil {
		conf.errors = int(o)
	}
}

// MaxErrors create q queue option for the maximum number of errors
func MaxErrors(limit int) Option {
	return maxErrors(limit)
}

// maxMessages is a queue option for configuring the max number of unacked messages
type maxMessages int

func (o maxMessages) Apply(conf *Config) {
	if conf != nil {
		conf.messages = int(o)
	}
}

// MaxMessages create q queue option for the maximum number of messages
func MaxMessages(limit int) Option {
	return maxMessages(limit)
}

// Config is a configuration object providing options for tuning the queue.
type Config struct {
	options struct {
		redis *redis.Client
		read  []redsync.Option
		write []redsync.Option
	}
	consumers int
	name      string
	interval  time.Duration
	errors    int
	messages  int
}

// Log instance with quiet support
type Log struct {
	quiet bool
}

// Log value
func (l Log) Log(level string, v ...interface{}) {
	if !l.quiet {
		tea.Log(level, v...)
	}
}

// Logf formatted value
func (l Log) Logf(level, format string, args ...interface{}) {
	if !l.quiet {
		tea.Logf(level, format, args...)
	}
}
