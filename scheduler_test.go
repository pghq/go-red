package red

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewScheduler(t *testing.T) {
	t.Parallel()

	queue := New(fmt.Sprintf("%s?queue=scheduler", queue.URL.String()))
	t.Run("can create instance", func(t *testing.T) {
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
	t.Parallel()

	queue := New(fmt.Sprintf("%s?queue=scheduler", queue.URL.String()))
	t.Run("can set new value", func(t *testing.T) {
		s := NewScheduler(queue).Every(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.interval)
	})
}

func TestScheduler_EnqueueTimeout(t *testing.T) {
	t.Parallel()

	queue := New(fmt.Sprintf("%s?queue=scheduler", queue.URL.String()))
	t.Run("can set new value", func(t *testing.T) {
		s := NewScheduler(queue).EnqueueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.enqueueTimeout)
	})
}

func TestScheduler_DequeueTimeout(t *testing.T) {
	t.Parallel()

	queue := New(fmt.Sprintf("%s?queue=scheduler", queue.URL.String()))
	t.Run("can set new value", func(t *testing.T) {
		s := NewScheduler(queue).DequeueTimeout(time.Second)
		assert.NotNil(t, s)
		assert.Equal(t, time.Second, s.dequeueTimeout)
	})
}

func TestScheduler_Add(t *testing.T) {
	t.Parallel()

	queue := New(fmt.Sprintf("%s?queue=scheduler", queue.URL.String()))
	t.Run("raises missing id errors", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("")
		s.Add(task)
		assert.NotNil(t, s)
		assert.Empty(t, s.tasks)
	})

	t.Run("can enqueue", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		s.Add(task)
		assert.NotNil(t, s)
		assert.NotEmpty(t, s.tasks)
		assert.Len(t, s.tasks, 1)
		assert.Equal(t, s.tasks[task.Id], task)
	})

	t.Run("does not add duplicates", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		s.Add(task).Add(task)
		assert.NotNil(t, s)
		assert.NotEmpty(t, s.tasks)
		assert.Len(t, s.tasks, 1)
		assert.Equal(t, s.tasks[task.Id], task)
	})
}

func TestScheduler_Start(t *testing.T) {
	t.Parallel()

	t.Run("raises enqueue errors", func(t *testing.T) {
		queue := New(fmt.Sprintf("%s?queue=enqueue:error", queue.URL.String()))
		mx := queue.Lock("enqueue:test")
		mx.Lock()
		defer mx.Unlock()
		s := NewScheduler(queue)
		task := s.NewTask("enqueue:test")
		s.Add(task)
		go s.Start()
		<-time.After(100 * time.Millisecond)
		s.Stop()
	})

	t.Run("ignores tasks not ready yet", func(t *testing.T) {
		queue := New(fmt.Sprintf("%s?queue=ignore", queue.URL.String()))
		s := NewScheduler(queue)
		task := s.NewTask("ignore:test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY")

		s.Add(task)
		go s.Start()
		<-time.After(100 * time.Millisecond)
		s.Stop()
		assert.False(t, task.IsComplete())
		assert.Equal(t, 0, task.Occurrences())
	})

	t.Run("schedules tasks that are ready", func(t *testing.T) {
		queue := New(fmt.Sprintf("%s?queue=ready", queue.URL.String()))
		s := NewScheduler(queue)
		task := s.NewTask("ready:test")
		s.Add(task)
		go s.Start()
		<-time.After(100 * time.Millisecond)
		s.Stop()
		assert.True(t, task.IsComplete())
		assert.Equal(t, 1, task.Occurrences())
	})
}

func TestScheduler_Worker(t *testing.T) {
	t.Parallel()

	t.Run("raises message errors", func(t *testing.T) {
		queue := New(fmt.Sprintf("%s?queue=message:error", queue.URL.String()))
		s := NewScheduler(queue)
		task := s.NewTask("error:test")
		s.Add(task).Add(task)
		s.Handle(func(task *Task) {})
		defer s.Stop()
		go s.Start()

		<-time.After(100 * time.Millisecond)
	})

	t.Run("can process tasks", func(t *testing.T) {
		queue := New(fmt.Sprintf("%s?queue=proccess", queue.URL.String()))
		s := NewScheduler(queue)
		task := s.NewTask("process:test")
		s.Add(task).Add(task)
		s.Handle(func(task *Task) {})
		defer s.Stop()
		go s.Start()
		<-time.After(200 * time.Millisecond)
	})
}

func TestTask_CanSchedule(t *testing.T) {
	t.Parallel()

	t.Run("tasks not after do not schedule", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		_ = task.SetRecurrence("UNTIL=19700101T000000Z;FREQ=DAILY")
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("tasks with bad recurrence do not schedule", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		task.Schedule.Recurrence = "DAILY"
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("tasks that have already reached limit do not schedule", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY;COUNT=1")
		task.Schedule.Count = 1
		canSchedule := task.CanSchedule(time.Now())
		assert.False(t, canSchedule)
	})

	t.Run("schedules tasks", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		_ = task.SetRecurrence("FREQ=DAILY;COUNT=1")
		canSchedule := task.CanSchedule(time.Now())
		assert.True(t, canSchedule)
		task.Unlock()
		task.Unlock()
	})
}

func TestTask_IsComplete(t *testing.T) {
	t.Parallel()

	t.Run("tasks that have ended are complete", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		_ = task.SetRecurrence("UNTIL=19700101T000000Z;FREQ=DAILY")
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})

	t.Run("tasks that have reached limit are complete", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		_ = task.SetRecurrence("DTSTART=99990101T000000Z;FREQ=DAILY;COUNT=1")
		task.Schedule.Count = 1
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})

	t.Run("tasks with bad recurrence are complete", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		task.Schedule.Recurrence = "DAILY"
		isComplete := task.IsComplete()
		assert.True(t, isComplete)
	})
}

func TestTask_SetRecurrence(t *testing.T) {
	t.Parallel()

	t.Run("does not set task with bad recurrence", func(t *testing.T) {
		s := NewScheduler(queue)
		task := s.NewTask("test")
		err := task.SetRecurrence("DAILY")
		assert.NotNil(t, err)
		assert.Empty(t, task.Schedule.Recurrence)
	})
}
