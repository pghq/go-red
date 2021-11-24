package red

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewScheduler(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		s := NewScheduler(queue)
		assert.NotNil(t, s)
		assert.Equal(t, s.queue, queue)
		assert.Equal(t, DefaultSchedulerInterval, s.interval)
		assert.Equal(t, DefaultEnqueueTimeout, s.enqueueTimeout)
		assert.Equal(t, DefaultDequeueTimeout, s.dequeueTimeout)
		assert.Empty(t, s.tasks)

		s.Stop()
		s.Stop()
	})
}

func TestScheduler_Every(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		s := NewScheduler(queue).Every(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.interval)
	})
}

func TestScheduler_EnqueueTimeout(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		s := NewScheduler(queue).EnqueueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.enqueueTimeout)
	})
}

func TestScheduler_DequeueTimeout(t *testing.T) {
	t.Run("can set new value", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		s := NewScheduler(queue).DequeueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.dequeueTimeout)
	})
}

func TestScheduler_Add(t *testing.T) {
	t.Run("raises missing id errors", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		task := NewTask("")
		s := NewScheduler(queue).Add(task)
		assert.NotNil(t, s)
		assert.Empty(t, s.tasks)
	})

	t.Run("can enqueue", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		task := NewTask("test")
		s := NewScheduler(queue).Add(task)
		assert.NotNil(t, s)
		assert.NotEmpty(t, s.tasks)
		assert.Len(t, s.tasks, 1)
		assert.Equal(t, s.tasks[task.Id], task)
	})

	t.Run("does not add duplicates", func(t *testing.T) {
		db, _, teardown := setup(t)
		defer teardown()

		queue, _ := NewQueue("", WithRedis(db))

		task := NewTask("test")
		s := NewScheduler(queue).Add(task).Add(task)
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
		mock.Regexp().ExpectSetNX("red.w.test", ".+", 8*time.Second).
			SetVal(false)

		queue, _ := NewQueue("", WithRedis(db), WithConsumers(0))

		task := NewTask("test")
		done := make(chan struct{}, 1)
		s := NewScheduler(queue).Add(task).Notify(func(t *Task) {
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

		queue, _ := NewQueue("", WithRedis(db), WithConsumers(0))

		task := NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY")

		done := make(chan struct{}, 1)
		s := NewScheduler(queue).Add(task).Notify(func(t *Task) {
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
		mock.Regexp().ExpectSetNX("red.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*red.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)

		queue, _ := NewQueue("", WithRedis(db), WithConsumers(0))
		task := NewTask("test")
		done := make(chan struct{}, 1)
		s := NewScheduler(queue).Add(task).Notify(func(t *Task) {
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
		mock.Regexp().ExpectSetNX("red.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*red.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)
		mock.Regexp().ExpectLLen(".*red.messages.*").
			SetVal(1)
		mock.Regexp().ExpectSetNX("red.r.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectRPopLPush(".*red.messages.*", ".*red.messages.*").
			SetVal(`{"id":"test","value":"YmFk"}`)
		mock.Regexp().ExpectLRem(".*red.messages.*", 1, `{"id":"test","value":"YmFk"}`).
			SetVal(0)

		queue, _ := NewQueue("", WithRedis(db), WithConsumers(1))

		task := NewTask("test")
		scheduled := make(chan struct{}, 1)
		done := make(chan struct{}, 1)
		s := NewScheduler(queue).Add(task).Add(task).
			Notify(func(t *Task) {
				scheduled <- struct{}{}
			}).
			NotifyWorker(func(msg *Message) {
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
		mock.Regexp().ExpectSetNX("red.w.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectLPush(".*red.messages.*", `{"id":"test","value":".+"}`).
			SetVal(1)
		mock.Regexp().ExpectLLen(".*red.messages.*").
			SetVal(1)
		mock.Regexp().ExpectSetNX("red.r.test", ".+", 8*time.Second).
			SetVal(true)
		mock.Regexp().ExpectRPopLPush(".*red.messages.*", ".*red.messages.*").
			SetVal(`{"id":"test","value":"eyJpZCI6ICJ0ZXN0In0="}`)
		mock.Regexp().ExpectLRem(".*red.messages.*", 1, `{"id":"test","value":"eyJpZCI6ICJ0ZXN0In0="}`).
			SetVal(1)

		queue, _ := NewQueue("", WithRedis(db), WithConsumers(1))

		task := NewTask("test")
		scheduled := make(chan struct{}, 1)
		done := make(chan struct{}, 1)
		s := NewScheduler(queue).Add(task).Add(task).
			Notify(func(t *Task) {
				scheduled <- struct{}{}
			}).
			NotifyWorker(func(msg *Message) {
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
		task.Unlock()
		task.Unlock()
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
