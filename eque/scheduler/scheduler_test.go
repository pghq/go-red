package scheduler

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/pghq/go-eque/eque"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/pghq/go-museum/museum/diagnostic/log"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	log.Writer(io.Discard)
	defer log.Reset()
	code := m.Run()
	os.Exit(code)
}

func TestNew(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		s := New(queue)
		assert.NotNil(t, s)
		assert.Equal(t, s.queue, queue)
		assert.Equal(t, DefaultInterval, s.interval)
		assert.Equal(t, DefaultEnqueueTimeout, s.enqueueTimeout)
		assert.Equal(t, DefaultDequeueTimeout, s.dequeueTimeout)
		assert.Empty(t, s.tasks)
	})
}

func TestScheduler_Every(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		s := New(queue).Every(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.interval)
	})
}

func TestScheduler_EnqueueTimeout(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		s := New(queue).EnqueueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.enqueueTimeout)
	})
}

func TestScheduler_DequeueTimeout(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		s := New(queue).DequeueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.dequeueTimeout)
	})
}

func TestScheduler_Add(t *testing.T) {
	t.Run("raises missing id errors", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		task := NewTask("")
		s := New(queue).Add(task)
		assert.NotNil(t, s)
		assert.Empty(t, s.tasks)
	})

	t.Run("can enqueue", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		task := NewTask("test")
		s := New(queue).Add(task)
		assert.NotNil(t, s)
		assert.NotEmpty(t, s.tasks)
		assert.Len(t, s.tasks, 1)
		assert.Equal(t, s.tasks[task.Id], task)
	})

	t.Run("does not add duplicates", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db))

		task := NewTask("test")
		s := New(queue).Add(task).Add(task)
		assert.NotNil(t, s)
		assert.NotEmpty(t, s.tasks)
		assert.Len(t, s.tasks, 1)
		assert.Equal(t, s.tasks[task.Id], task)
	})
}

func TestScheduler_Start(t *testing.T) {
	t.Run("raises enqueue errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8*time.Second).
			SetVal(false)

		queue, _ := eque.New("", eque.WithRedis(db), eque.WithConsumers(0))

		task := NewTask("test")
		done := make(chan struct{}, 1)
		s := New(queue).Add(task).Notify(func(t *Task) {
			done <- struct{}{}
		})
		go s.Start()
		<-done
		s.Stop()
		assert.False(t, task.IsComplete())
	})

	t.Run("ignores tasks not ready yet", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := eque.New("", eque.WithRedis(db), eque.WithConsumers(0))

		task := NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY")

		done := make(chan struct{}, 1)
		s := New(queue).Add(task).Notify(func(t *Task) {
			done <- struct{}{}
		})

		go s.Start()
		<-done
		s.Stop()
		assert.False(t, task.IsComplete())
		assert.Equal(t, 0, task.Occurrences())
	})

	t.Run("schedules tasks that are ready", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 0)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*eque.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)

		queue, _ := eque.New("", eque.WithRedis(db), eque.WithConsumers(0))
		task := NewTask("test")
		done := make(chan struct{}, 1)
		s := New(queue).Add(task).Notify(func(t *Task) {
			done <- struct{}{}
		})
		go s.Start()
		<-done
		s.Stop()
		assert.True(t, task.IsComplete())
		assert.Equal(t, 1, task.Occurrences())
	})
}

func TestScheduler_Worker(t *testing.T) {
	t.Run("raises message errors", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 1)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*eque.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)
		mock.Regexp().ExpectLLen(".*eque.messages.*").
			SetVal(1)
		mock.Regexp().ExpectSetNX("eque.r.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectRPopLPush(".*eque.messages.*", ".*eque.messages.*").
			SetVal(`{"id":"test","value":"YmFk"}`)
		mock.Regexp().ExpectLRem(".*eque.messages.*", 1, `{"id":"test","value":"YmFk"}`).
			SetVal(0)

		queue, _ := eque.New("", eque.WithRedis(db), eque.WithConsumers(1))

		task := NewTask("test")
		scheduled := make(chan struct{}, 1)
		done := make(chan struct{}, 1)
		s := New(queue).Add(task).Add(task).
			Notify(func(t *Task) {
				scheduled <- struct{}{}
			}).
			NotifyWorker(func(msg *eque.Message) {
				if msg != nil {
					done <- struct{}{}
				}
			})
		defer s.Stop()
		go s.Start()

		w := s.Worker(func(_ *Task) {})
		<-scheduled
		go w.Start()
		defer w.Stop()
		<-done
	})

	t.Run("can process tasks", func(t *testing.T) {
		db, mock, teardown := setup(t)
		defer teardown()

		expectConsumers(mock, 1)
		mock.Regexp().ExpectSetNX("eque.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*eque.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)
		mock.Regexp().ExpectLLen(".*eque.messages.*").
			SetVal(1)
		mock.Regexp().ExpectSetNX("eque.r.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectRPopLPush(".*eque.messages.*", ".*eque.messages.*").
			SetVal(`{"id":"test","value":"eyJpZCI6ICJ0ZXN0In0="}`)
		mock.Regexp().ExpectLRem(".*eque.messages.*", 1, `{"id":"test","value":"eyJpZCI6ICJ0ZXN0In0="}`).
			SetVal(1)

		queue, _ := eque.New("", eque.WithRedis(db), eque.WithConsumers(1))

		task := NewTask("test")
		scheduled := make(chan struct{}, 1)
		done := make(chan struct{}, 1)
		s := New(queue).Add(task).Add(task).
			Notify(func(t *Task) {
				scheduled <- struct{}{}
			}).
			NotifyWorker(func(msg *eque.Message) {
				if msg != nil {
					done <- struct{}{}
				}
			})
		defer s.Stop()
		go s.Start()

		w := s.Worker(func(_ *Task) {})
		<-scheduled
		go w.Start()
		defer w.Stop()
		<-done
	})
}

func TestTask_CanSchedule(t *testing.T) {
	t.Run("tasks that have do not schedule", func(t *testing.T) {
		task := NewTask("test")
		_ = task.SetRecurrence("UNTIL=19700101T000000Z;FREQ=DAILY")
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("tasks with bad recurrence do not schedule", func(t *testing.T) {
		task := NewTask("test")
		task.Schedule.Recurrence = "DAILY"
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("tasks that have already reached limit do not schedule", func(t *testing.T) {
		task := NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY;COUNT=1")
		task.Schedule.Count = 1
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("schedules tasks", func(t *testing.T) {
		task := NewTask("test")
		_ = task.SetRecurrence("FREQ=DAILY;COUNT=1")
		canSchedule := task.CanSchedule(time.Now())
		assert.True(t, canSchedule)
	})
}

func TestTask_IsComplete(t *testing.T) {
	t.Run("tasks that have ended are complete", func(t *testing.T) {
		task := NewTask("test")
		_ = task.SetRecurrence("UNTIL=19700101T000000Z;FREQ=DAILY")
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})

	t.Run("tasks that have reached limit are complete", func(t *testing.T) {
		task := NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY;COUNT=1")
		task.Schedule.Count = 1
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})

	t.Run("tasks with bad recurrence are complete", func(t *testing.T) {
		task := NewTask("test")
		task.Schedule.Recurrence = "DAILY"
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})
}

func TestTask_SetRecurrence(t *testing.T) {
	t.Run("does not set task with bad recurrence", func(t *testing.T) {
		task := NewTask("test")
		err := task.SetRecurrence("DAILY")
		assert.NotNil(t, err)
		assert.Empty(t, task.Schedule.Recurrence)
	})
}

func setup(t *testing.T) (*redis.Client, redismock.ClientMock, func()) {
	t.Helper()
	db, mock := redismock.NewClientMock()
	teardown := func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Error(err)
		}
	}

	return db, mock, teardown
}

func expectConsumers(mock redismock.ClientMock, count int) {
	mock.MatchExpectationsInOrder(false)
	mock.Regexp().ExpectSet(".*", ".*", time.Minute).SetVal("ok")
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)
	mock.Regexp().ExpectSAdd(".*", ".*eque.messages.*").SetVal(1)

	for i := 1; i <= count; i++ {
		member := fmt.Sprintf(`.*eque.consumer.%d\.*`, i)
		mock.Regexp().ExpectSAdd(".*", member).SetVal(1)
	}
}
