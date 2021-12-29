package red

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/alicebob/miniredis/v2"
	"github.com/pghq/go-tea"
	"github.com/stretchr/testify/assert"
)

var queue *Red

func TestMain(m *testing.M) {
	tea.Testing()
	s, _ := miniredis.Run()
	defer s.Close()
	queue = New(fmt.Sprintf("redis://%s?queue=test", s.Addr()))
	os.Exit(m.Run())
}

func TestRed(t *testing.T) {
	t.Parallel()

	t.Run("raises queue connection errors", func(t *testing.T) {
		t.Run("enqueue", func(t *testing.T) {
			queue := New("")
			assert.NotNil(t, queue)
			assert.NotNil(t, queue.Enqueue(context.Background(), "", ""))
		})

		t.Run("dequeue", func(t *testing.T) {
			queue := New("")
			assert.NotNil(t, queue)
			_, err := queue.Dequeue(context.Background())
			assert.NotNil(t, err)
		})
	})

	t.Run("can send messages", func(t *testing.T) {
		msg := &Message{}
		for i := 0; i <= 1024; i++ {
			queue.send(msg)
		}
		for i := 0; i < 1024; i++ {
			assert.NotNil(t, queue.message())
		}
		assert.Nil(t, queue.message())
		assert.NotNil(t, queue.Error())
	})

	t.Run("can send errors", func(t *testing.T) {
		err := tea.Err("an error has occurred")
		for i := 0; i <= 1024; i++ {
			queue.sendError(err)
		}
		for i := 0; i < 1024; i++ {
			assert.NotNil(t, queue.Error())
		}
		assert.Nil(t, queue.Error())
	})

	t.Run("raises consumption errors", func(t *testing.T) {
		queue.consume(&badDelivery{})
		assert.NotNil(t, queue.Error())
	})

	t.Run("can consume deliveries", func(t *testing.T) {
		queue.consume(&goodDelivery{})
		assert.NotNil(t, queue.message())
	})

	t.Run("can decode messages", func(t *testing.T) {
		msg := Message{Id: "test", Value: []byte(`{"key": "value"}`)}
		var value struct {
			Value string `json:"key"`
		}
		err := msg.Decode(&value)
		assert.Nil(t, err)
		assert.Equal(t, "value", value.Value)
	})

	t.Run("message raises ack errors", func(t *testing.T) {
		msg := Message{
			ack: func() error { return tea.Err("an error has occurred") },
		}

		err := msg.Ack(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("can ack message", func(t *testing.T) {
		msg := Message{
			Id:    "test",
			lock:  queue.Lock,
			rlock: queue.RLock,
			ack:   func() error { return nil },
		}

		err := msg.Ack(context.TODO())
		assert.Nil(t, err)
	})

	t.Run("message raises reject errors", func(t *testing.T) {
		msg := Message{
			reject: func() error { return tea.Err("an error has occurred") },
		}

		err := msg.Reject(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("can reject message", func(t *testing.T) {
		msg := Message{
			Id:    "test",
			lock:  queue.Lock,
			rlock: queue.RLock,
			ack:   func() error { return nil },
		}

		err := msg.Reject(context.TODO())
		assert.Nil(t, err)
	})

	t.Run("can enqueue", func(t *testing.T) {
		err := queue.Enqueue(context.TODO(), "ok:test", "value")
		assert.Nil(t, err)
	})

	t.Run("can dequeue", func(t *testing.T) {
		<-time.After(100 * time.Millisecond)
		m, err := queue.Dequeue(context.TODO())
		assert.Nil(t, err)
		assert.NotNil(t, m)
		assert.Equal(t, "ok:test", m.Id)
	})

	t.Run("can schedule", func(t *testing.T) {
		queue.StartScheduling(func(task *Task) {}, func() {})
		defer queue.StopScheduling()
		queue.Once("test")
		assert.NotNil(t, queue.Repeat("test", "DAILY"))
		assert.Nil(t, queue.Repeat("test", "FREQ=DAILY;COUNT=1"))
		queue.Wait()
	})

	t.Run("enqueue raises busy lock errors", func(t *testing.T) {
		_ = queue.Enqueue(context.TODO(), "busy:test", "value")
		err := queue.Enqueue(context.TODO(), "busy:test", "value")
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("enqueue raises bad value errors", func(t *testing.T) {
		err := queue.Enqueue(context.TODO(), "bad:test", func() {})
		assert.NotNil(t, err)
		assert.False(t, tea.IsFatal(err))
	})

	t.Run("dequeue raises ctx errors", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		_, err := queue.Dequeue(ctx)
		assert.NotNil(t, err)
	})

	t.Run("dequeue raises empty queue errors", func(t *testing.T) {
		for {
			if _, err := queue.Dequeue(context.TODO()); err != nil {
				break
			}
		}
	})

	t.Run("dequeue handles read lock errors", func(t *testing.T) {
		mutex := queue.RLock("dequeue:test")
		mutex.Lock()
		defer mutex.Unlock()

		queue.send(&Message{Id: "dequeue:test", reject: func() error { return tea.Err("an error") }})
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
		defer cancel()
		_, err := queue.Dequeue(ctx)
		assert.NotNil(t, err)
	})
}

// badDelivery is a partial mock for rmq deliveries with bad json
type badDelivery struct {
	rmq.Delivery
}

func (d badDelivery) Payload() string {
	return ""
}

// goodDelivery is a partial mock for rmq deliveries with good json
type goodDelivery struct {
	rmq.Delivery
}

func (d goodDelivery) Payload() string {
	return "{}"
}
