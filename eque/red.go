package eque

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

const (
	// DefaultConsumers is the total number of consumers processing messages
	DefaultConsumers = 10

	// DefaultName is the default queue name
	DefaultName = "eque.messages"

	// DefaultInterval is the duration the queue sleeps before checking for new deliveries
	DefaultInterval = 100 * time.Millisecond

	// DefaultMaxMessages is the default max number of messages to keep before dropping occurs
	DefaultMaxMessages = 1024

	// DefaultMaxErrors is the default max number of errors to track before dropping occurs
	DefaultMaxErrors = 1024
)

// Red is an instance of the exclusive message queue.
type Red struct {
	queue rmq.Queue
	messages chan *Message
	errors chan error
	errorSize int
	messageSize int
	pool *redsync.Redsync
	readOptions []redsync.Option
	writeOptions []redsync.Option
}

// New creates an instance of the exclusive queue
func New(addr string, opts... Option) (*Red, error) {
	conf := Config{
		name: DefaultName,
		consumers: DefaultConsumers,
		interval: DefaultInterval,
		messages: DefaultMaxMessages,
		errors: DefaultMaxErrors,
	}

	for _, opt := range opts{
		opt.Apply(&conf)
	}

	client := conf.options.redis
	if client == nil{
		client = redis.NewClient(&redis.Options{
			Addr: addr,
		})
	}

	pool := goredis.NewPool(client)
	q := Red{
		messages: make(chan *Message, conf.messages),
		errors: make(chan error, conf.errors),
		pool: redsync.New(pool),
		readOptions: conf.options.read,
		writeOptions: conf.options.write,
	}

	conn, err := rmq.OpenConnectionWithRedisClient(conf.name, client, q.errors)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	mq, err := conn.OpenQueue(conf.name)
	if err != nil {
		return nil, errors.Wrap(err)
	}

	if err := mq.StartConsuming(int64(conf.consumers + 1), conf.interval); err != nil {
		return nil, errors.Wrap(err)
	}

	q.queue = mq
	for i := 0; i < conf.consumers; i++ {
		tag := fmt.Sprintf("eque.consumer.%d", i + 1)
		_, err = mq.AddConsumerFunc(tag, q.consume)

		if err != nil {
			return nil, errors.Wrap(err)
		}
	}

	return &q, nil
}

// Send batches messages
func (q *Red) Send(msg *Message) *Red{
	select {
	case q.messages <- msg:
	default:
	}

	return q
}

// Message pops a message from the queue
func (q *Red) Message() *Message{
	select {
	case msg := <-q.messages:
		return msg
	default:
		return nil
	}
}

// SendError batches errors
func (q *Red) SendError(err error) *Red{
	select {
	case q.errors <- err:
	default:
	}

	return q
}

// Error checks if any errors have occurred
func (q *Red) Error() error{
	select {
	case err := <- q.errors:
		return err
	default:
		return nil
	}
}

// consume is handles rmq deliveries
func (q *Red) consume(delivery rmq.Delivery){
	var msg Message
	if err := json.Unmarshal([]byte(delivery.Payload()), &msg); err != nil {
		q.SendError(errors.Wrap(err))
		return
	}

	msg.ack = delivery.Ack
	msg.reject = delivery.Reject
	msg.pool = q.pool
	msg.readOptions = q.readOptions
	msg.writeOptions = q.writeOptions
	q.Send(&msg)
}

// Config is a configuration object providing options for tuning the queue.
type Config struct{
	options struct{
		redis *redis.Client
		read []redsync.Option
		write []redsync.Option
	}
	consumers int
	name string
	interval time.Duration
	errors int
	messages int
}

