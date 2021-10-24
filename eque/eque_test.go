package eque

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/stretchr/testify/assert"
)

func TestRed(t *testing.T){
	t.Run("raises queue connection errors", func(t *testing.T) {
		queue, err := New("")
		assert.NotNil(t, err)
		assert.Nil(t, queue)
	})

	t.Run("raises queue open errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()
		mock.Regexp().ExpectSet(".*", ".*", time.Minute).SetVal("ok")
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetErr(errors.New("an error has occurred"))
		queue, err := New("", WithRedis(db))
		assert.NotNil(t, err)
		assert.Nil(t, queue)
	})

	t.Run("raises queue consumption errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()
		mock.Regexp().ExpectSet(".*", ".*", time.Minute).SetVal("ok")
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetErr(errors.New("an error has occurred"))
		queue, err := New("", WithRedis(db))
		assert.NotNil(t, err)
		assert.Nil(t, queue)
	})

	t.Run("raises consumer func errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()
		mock.Regexp().ExpectSet(".*", ".*", time.Minute).SetVal("ok")
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
		mock.Regexp().ExpectSAdd(".*", `.*eque.consumer.1\.*`).SetErr(errors.New("an error has occurred"))

		queue, err := New("", WithRedis(db), WithConsumers(1))
		assert.NotNil(t, err)
		assert.Nil(t, queue)
	})

	t.Run("can create instance", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 1)

		opts := []Option{
			WithRedis(db),
			WithConsumers(1),
			Read(redsync.WithExpiry(time.Second)),
			Write(redsync.WithExpiry(time.Second)),
			Name("eque.messages"),
			At(time.Millisecond),
		}
		queue, err := New("", opts...)
		assert.Nil(t, err)
		assert.NotNil(t, queue)
		assert.Equal(t, 1, len(queue.readOptions))
		assert.Equal(t, 1, len(queue.writeOptions))
	})

	t.Run("can send messages", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 1)
		queue, _ := New("", WithRedis(db), WithConsumers(1), MaxMessages(1))
		msg := &Message{}
		msg = queue.Send(msg).Send(msg).Message()
		assert.NotNil(t, msg)
		assert.Nil(t, queue.Message())
	})

	t.Run("can send errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 1)
		queue, _ := New("", WithRedis(db), WithConsumers(1), MaxErrors(1))
		err := errors.New("an error has occurred")
		err = queue.SendError(err).SendError(err).Error()
		assert.NotNil(t, err)
		assert.Nil(t, queue.Error())
	})

	t.Run("raises consumption errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		queue.consume(&badDelivery{})
		assert.NotNil(t, queue.Error())
	})

	t.Run("can consume deliveries", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		queue.consume(&goodDelivery{})
		assert.NotNil(t, queue.Message())
	})

	t.Run("can decode messages", func(t *testing.T) {
		msg := Message{Id: "test", Value: []byte(`{"key": "value"}`)}
		var value struct{
			Value string `json:"key"`
		}
		err := msg.Decode(&value)
		assert.Nil(t, err)
		assert.Equal(t, "value", value.Value)
	})

	t.Run("message raises ack errors", func(t *testing.T) {
		msg := Message{
			ack: func() error { return errors.New("an error has occurred") },
		}

		err := msg.Ack(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("message raises ack write unlock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetErr(errors.New("an error has occurred"))

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Ack(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("message raises ack read unlock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetVal(1)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.r.test"}, "").
			SetErr(errors.New("an error has occurred"))

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Ack(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("can ack message", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetVal(1)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.r.test"}, "").
			SetVal(1)

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Ack(context.TODO())
		assert.Nil(t, err)
	})

	t.Run("message raises reject errors", func(t *testing.T) {
		msg := Message{
			reject: func() error { return errors.New("an error has occurred") },
		}

		err := msg.Reject(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("message raises reject write unlock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetErr(errors.New("an error has occurred"))

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Reject(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("message raises reject read unlock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetVal(1)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.r.test"}, "").
			SetErr(errors.New("an error has occurred"))

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Reject(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("can reject message", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.w.test"}, "").
			SetVal(1)
		mock.Regexp().ExpectEvalSha(".+", []string{"eque.r.test"}, "").
			SetVal(1)

		queue, _ := New("", WithRedis(db), WithConsumers(0))

		msg := Message{
			Id: "test",
			pool: queue.pool,
			ack: func() error { return nil },
		}

		err := msg.Reject(context.TODO())
		assert.Nil(t, err)
	})

	t.Run("enqueue raises busy lock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8 * time.Second).
			SetVal(false)
		queue, _ := New("", WithRedis(db), WithConsumers(0), Write(redsync.WithTries(1)))

		err := queue.Enqueue(context.TODO(), "test", "value")
		assert.NotNil(t, err)
		assert.False(t, errors.IsFatal(err))
	})

	t.Run("enqueue raises unknown lock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8 * time.Second).
			SetErr(errors.New("an error has occurred"))
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		err := queue.Enqueue(context.TODO(), "test", "value")
		assert.NotNil(t, err)
		assert.True(t, errors.IsFatal(err))
	})

	t.Run("enqueue raises bad value errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8 * time.Second).
			SetVal(true)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		err := queue.Enqueue(context.TODO(), "test", func(){})
		assert.NotNil(t, err)
		assert.False(t, errors.IsFatal(err))
	})

	t.Run("enqueue raises queue publish errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8 * time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*eque.messages.*", `{"id":"test","value":".+"}`).
			SetErr(errors.New("an error has occurred"))
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		err := queue.Enqueue(context.TODO(), "test", "value")
		assert.NotNil(t, err)
		assert.True(t, errors.IsFatal(err))
	})

	t.Run("can enqueue", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8 * time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*eque.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		err := queue.Enqueue(context.TODO(), "test", "value")
		assert.Nil(t, err)
	})

	t.Run("dequeue raises ctx errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		ctx, cancel := context.WithTimeout(context.Background(), 0)
		defer cancel()
		_, err := queue.Dequeue(ctx)
		assert.NotNil(t, err)
	})

	t.Run("dequeue raises empty queue errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		_, err := queue.Dequeue(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("dequeue handles read lock errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.r.test", ".+", 8 * time.Second).
			SetErr(errors.New("an error has occurred"))
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		queue.Send(&Message{Id: "test", reject: func() error {
			return errors.New("an error has occurred")
		}})
		_, err := queue.Dequeue(context.TODO())
		assert.NotNil(t, err)
	})

	t.Run("can dequeue", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.r.test", ".+", 8 * time.Second).
			SetVal(true)
		queue, _ := New("", WithRedis(db), WithConsumers(0))

		queue.Send(&Message{Id: "test", reject: func() error {
			return errors.New("an error has occurred")
		}})
		m, err := queue.Dequeue(context.TODO())
		assert.Nil(t, err)
		assert.NotNil(t, m)
		assert.Equal(t, "test", m.Id)
	})

}

func setup(t *testing.T) (*redis.Client, redismock.ClientMock, func()){
	t.Helper()
	db, mock := redismock.NewClientMock()
	teardown := func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	}

	return db, mock, teardown
}

func expectConsumers(mock redismock.ClientMock, count int){
	mock.MatchExpectationsInOrder(false)
	mock.Regexp().ExpectSet(".*", ".*", time.Minute).SetVal("ok")
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)

	for i := 1; i <= count; i++{
		member := fmt.Sprintf(`.*eque.consumer.%d\.*`, i)
		mock.Regexp().ExpectSAdd(".*", member).SetVal(1)
	}
}

// badDelivery is a partial mock for rmq deliveries with bad json
type badDelivery struct {
	rmq.Delivery
}

func (d badDelivery) Payload() string{
	return ""
}

// goodDelivery is a partial mock for rmq deliveries with good json
type goodDelivery struct {
	rmq.Delivery
}

func (d goodDelivery) Payload() string{
	return "{}"
}
