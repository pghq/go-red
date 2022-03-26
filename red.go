package red

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/pghq/go-tea"
)

const (
	// Version of the queue
	Version = "0.0.36"

	// Prefix is the name prefix of the queue
	Prefix = "go-red/v" + Version

	// BatchSize the total number of errors and messages consumed before dropping occurs
	BatchSize = 1024
)

// Red is an instance of the exclusive message queue.
type Red struct {
	Name      string
	URL       *url.URL
	queue     rmq.Queue
	sync      *redsync.Redsync
	messages  chan *Message
	errors    chan error
	waits     chan struct{}
	scheduler *Scheduler
	worker    *Worker
}

// Once schedules a task to be done once
func (r Red) Once(key string) {
	r.scheduler.Add(r.scheduler.NewTask(key))
}

// Repeat schedules a task to be done at least once
func (r Red) Repeat(key, recurrence string) error {
	task := r.scheduler.NewTask(key)
	if err := task.SetRecurrence(recurrence); err != nil {
		return tea.Stacktrace(err)
	}
	r.scheduler.Add(task)

	return nil
}

// Wait for next task to be handled
func (r Red) Wait() {
	r.waits <- struct{}{}
}

// StartScheduling tasks
func (r Red) StartScheduling(handler func(task *Task), schedulers ...func()) {
	r.scheduler.Handle(func(task *Task) {
		handler(task)
		for {
			select {
			case <-r.waits:
			default:
				return
			}
		}
	})

	r.worker.AddJobs(schedulers...)
	go r.worker.Start()
	go r.scheduler.Start()
}

// StopScheduling tasks
func (r Red) StopScheduling() {
	r.worker.Stop()
	r.scheduler.Stop()
}

// Error checks if any errors have occurred
func (r Red) Error() error {
	select {
	case err := <-r.errors:
		return err
	default:
		return nil
	}
}

// Lock attempts to obtain a named lock for read/write
func (r Red) Lock(name string, opts ...redsync.Option) *redsync.Mutex {
	opts = append([]redsync.Option{redsync.WithTries(3)}, opts...)
	return r.sync.NewMutex(fmt.Sprintf("%s/write/%s", r.Name, name), opts...)
}

// RLock attempts to obtain a named lock for reading
func (r Red) RLock(name string, opts ...redsync.Option) *redsync.Mutex {
	opts = append([]redsync.Option{redsync.WithTries(3)}, opts...)
	return r.sync.NewMutex(fmt.Sprintf("%s/read/%s", r.Name, name), opts...)
}

// send batches messages
func (r Red) send(msg *Message) {
	select {
	case r.messages <- msg:
	default:
		r.sendError(tea.Err("message dropped for: ", msg.Id))
	}
}

// message pops a message from the queue
func (r Red) message() *Message {
	select {
	case msg := <-r.messages:
		return msg
	default:
		return nil
	}
}

// sendError batches errors
func (r Red) sendError(err error) {
	select {
	case r.errors <- err:
	default:
	}
}

// consume is handles rmq deliveries
func (r Red) consume(delivery rmq.Delivery) {
	var msg Message
	if err := json.Unmarshal([]byte(delivery.Payload()), &msg); err != nil {
		r.sendError(tea.Stacktrace(err))
		return
	}

	msg.ack = delivery.Ack
	msg.reject = delivery.Reject
	msg.lock = r.Lock
	msg.rlock = r.RLock
	r.send(&msg)
}

// New creates a named instance of the exclusive queue
func New(redisURL string) *Red {
	q := Red{
		Name:     Prefix,
		messages: make(chan *Message, BatchSize),
		errors:   make(chan error, BatchSize),
		waits:    make(chan struct{}),
	}

	q.URL, _ = url.Parse(redisURL)
	if q.URL != nil {
		query := q.URL.Query()
		q.Name = strings.ToLower(strings.Join([]string{q.Name, query.Get("queue")}, "/"))
		query.Del("queue")
		q.URL.RawQuery = query.Encode()
		redisURL = q.URL.String()
	}

	var conn rmq.Connection
	options, err := redis.ParseURL(redisURL)
	if err == nil {
		client := redis.NewClient(options)
		q.sync = redsync.New(goredis.NewPool(client))
		conn, err = rmq.OpenConnectionWithRedisClient(q.Name, client, q.errors)
	}

	if err == nil {
		q.queue, err = conn.OpenQueue(q.Name)
	}

	if err == nil {
		err = q.queue.StartConsuming(11, time.Millisecond)
	}

	if err == nil {
		for i := 0; i < 10; i++ {
			tag := fmt.Sprintf("consumer/#%d", i+1)
			if err == nil {
				_, err = q.queue.AddConsumerFunc(tag, q.consume)
			}
		}
	}

	if err != nil {
		q.errors <- tea.Stacktrace(err)
	}

	q.worker = NewWorker("red").Every(100 * time.Millisecond)
	q.scheduler = NewScheduler(&q)
	return &q
}

// Message is a single instance of a message within the queue.
type Message struct {
	Id    string `json:"id"`
	Value []byte `json:"value"`

	lock   func(string, ...redsync.Option) *redsync.Mutex
	rlock  func(string, ...redsync.Option) *redsync.Mutex
	ack    func() error
	reject func() error
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

	w := m.lock(m.Id)
	_, err := w.UnlockContext(ctx)
	if err == nil {
		r := m.rlock(m.Id)
		_, err = r.UnlockContext(ctx)
	}

	return err
}

// Reject notifies upstream of unsuccessful message handling
func (m *Message) Reject(ctx context.Context) error {
	if m.reject != nil {
		if err := m.reject(); err != nil {
			return err
		}
	}

	w := m.lock(m.Id)
	_, err := w.UnlockContext(ctx)
	if err == nil {
		r := m.rlock(m.Id)
		_, err = r.UnlockContext(ctx)
	}

	return err
}
