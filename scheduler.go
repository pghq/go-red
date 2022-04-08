package red

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-tea/trail"
	"github.com/teambition/rrule-go"
)

const (
	// DefaultSchedulerInterval is the default period between checking tasks to schedule
	DefaultSchedulerInterval = time.Millisecond

	// DefaultEnqueueTimeout is the default time allowed for queue write ops
	DefaultEnqueueTimeout = 100 * time.Millisecond

	// DefaultDequeueTimeout is the default time allowed for queue read ops
	DefaultDequeueTimeout = 100 * time.Millisecond
)

// Scheduler is an instance of a persistent background scheduler
type Scheduler struct {
	worker         *Worker
	interval       time.Duration
	stop           chan struct{}
	queue          *Red
	enqueueTimeout time.Duration
	dequeueTimeout time.Duration
	rwlock         sync.RWMutex
	tasks          map[string]*Task
	completed      chan *Task
	wg             sync.WaitGroup
}

// Every sets the interval for checking for new jobs to scheduler.
func (s *Scheduler) Every(interval time.Duration) *Scheduler {
	s.interval = interval

	return s
}

// EnqueueTimeout sets the maximum time to wait for adding items to queue.
func (s *Scheduler) EnqueueTimeout(timeout time.Duration) *Scheduler {
	s.enqueueTimeout = timeout

	return s
}

// DequeueTimeout sets the maximum time to wait for remove items from queue.
func (s *Scheduler) DequeueTimeout(timeout time.Duration) *Scheduler {
	s.dequeueTimeout = timeout

	return s
}

// Start begins scheduling tasks.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())

	s.wg.Add(1)
	go s.start(ctx)
	go s.worker.Start()

	trail.Info("red.scheduler: started")
	<-s.stop
	cancel()

	go func() {
		s.wg.Wait()
		s.worker.Stop()
		s.Stop()
	}()

	<-s.stop
	trail.Info("red.scheduler: stopped")
}

// Stop stops the scheduler and waits for background jobs to finish.
func (s *Scheduler) Stop() {
	select {
	case s.stop <- struct{}{}:
	default:
	}
}

// Add adds a task to be scheduled.
func (s *Scheduler) Add(tasks ...*Task) *Scheduler {
	for _, task := range tasks {
		if task.Id == "" {
			continue
		}

		s.rwlock.RLock()
		_, present := s.tasks[task.Id]
		s.rwlock.RUnlock()
		if present {
			trail.Debugf("red.scheduler: task=%s already in ledger", task.Id)
			continue
		}

		s.rwlock.Lock()
		s.tasks[task.Id] = task
		s.rwlock.Unlock()
		trail.Infof("red.scheduler: task=%s added to ledger", task.Id)
	}

	return s
}

// Handle tasks when scheduled
func (s *Scheduler) Handle(fn func(task *Task)) {
	defer func() {
		trail.Error(recover())
	}()

	h := func() {
		trail.Debug("red.scheduler.worker: started handling")
		for {
			ctx, cancel := context.WithTimeout(context.Background(), s.dequeueTimeout)
			msg, err := s.queue.Dequeue(ctx)
			cancel()
			if err != nil {
				break
			}

			go func() {
				trail.Infof("red.scheduler.worker: item=%s", msg.Id)
				defer func() {
					if err := msg.Ack(ctx); err != nil {
						trail.Error(err)
					}
				}()

				var task Task
				if err := msg.Decode(&task); err == nil {
					fn(&task)
					trail.Infof("red.scheduler.worker: task=%s handled", task.Id)
				}
			}()
		}
		trail.Debug("scheduler.worker.handle: finished handling")
	}

	s.worker.AddJobs(h)
}

// start background tasks and actually do the scheduling
func (s *Scheduler) start(ctx context.Context) {
	defer s.wg.Done()
	go func() {
		for {
			select {
			case <-ctx.Done():
				trail.Info("red.scheduler: background task #2 stopped")
				return
			case <-time.After(s.interval):
			}

			now := time.Now()
			s.rwlock.RLock()
			for _, task := range s.tasks {
				go func(task *Task) {
					ctx, cancel := context.WithTimeout(ctx, s.enqueueTimeout)
					defer cancel()

					if err := task.LockContext(ctx); err != nil {
						return
					}

					defer task.UnlockContext(ctx)
					if !task.CanSchedule(now) {
						return
					}

					if err := s.queue.Enqueue(ctx, task.Id, task); err != nil {
						trail.Error(err)
						return
					}

					task.MarkScheduled(now)
					if task.IsComplete() {
						s.completed <- task
					}

					trail.Infof("red.scheduler: task=%s scheduled", task.Id)
				}(task)
			}
			s.rwlock.RUnlock()
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				trail.Info("red.scheduler: background task #3 stopped")
				return
			case <-time.After(s.interval):
			}

			for removing := true; removing; {
				select {
				case task := <-s.completed:
					s.rwlock.Lock()
					delete(s.tasks, task.Id)
					s.rwlock.Unlock()
					trail.Infof("red.scheduler: task=%s removed from ledger", task.Id)
				default:
					removing = false
				}
			}
		}
	}()

	<-ctx.Done()
}

// NewScheduler creates a scheduler instance.
func NewScheduler(queue *Red) *Scheduler {
	s := Scheduler{
		worker:         NewWorker("scheduler"),
		queue:          queue,
		interval:       DefaultSchedulerInterval,
		enqueueTimeout: DefaultEnqueueTimeout,
		dequeueTimeout: DefaultDequeueTimeout,
		tasks:          make(map[string]*Task),
		completed:      make(chan *Task),
		stop:           make(chan struct{}, 1),
	}

	return &s
}

// Task is an instance of a thing to be scheduled.
type Task struct {
	Id       string       `json:"id"`
	Schedule TaskSchedule `json:"schedule"`
	*redsync.Mutex
}

// Occurrences gets the number of times the task has been scheduled.
func (t *Task) Occurrences() int {
	t.Schedule.RLock()
	defer t.Schedule.RUnlock()

	return t.Schedule.Count
}

// CanSchedule determines if the task can be scheduled at given time.
func (t *Task) CanSchedule(now time.Time) bool {
	t.Schedule.RLock()
	defer t.Schedule.RUnlock()

	if t.Schedule.Recurrence == "" {
		return t.Schedule.Count == 0
	}

	if rule, err := rrule.StrToRRule(t.Schedule.Recurrence); err == nil {
		if rule.Options.Count != 0 && t.Schedule.Count >= rule.Options.Count {
			return false
		}

		if now.After(rule.GetUntil()) {
			return false
		}

		if rule.Before(now, true) == rule.Before(t.Schedule.UpdatedAt, true) {
			return false
		}

		return true
	}

	return false
}

// MarkScheduled marks the task as scheduled.
func (t *Task) MarkScheduled(at time.Time) *Task {
	t.Schedule.Lock()
	defer t.Schedule.Unlock()

	t.Schedule.Count += 1
	t.Schedule.UpdatedAt = at
	return t
}

// IsComplete checks if the tasks can no longer be scheduled.
func (t *Task) IsComplete() bool {
	t.Schedule.RLock()
	defer t.Schedule.RUnlock()

	if t.Schedule.Recurrence == "" {
		return t.Schedule.Count != 0
	}

	now := time.Now()
	if rule, err := rrule.StrToRRule(t.Schedule.Recurrence); err == nil {
		if rule.Options.Count != 0 && t.Schedule.Count >= rule.Options.Count {
			return true
		}

		if now.After(rule.GetUntil()) {
			return true
		}

		return false
	}

	return true
}

// SetRecurrence sets a new recurrence rule based on rfc 5545
func (t *Task) SetRecurrence(rfc string) error {
	t.Schedule.Lock()
	defer t.Schedule.Unlock()

	if _, err := rrule.StrToRRule(rfc); err != nil {
		return trail.ErrorBadRequest(err)
	}

	t.Schedule.Recurrence = rfc

	return nil
}

// NewTask creates a new instance of a task to be scheduled.
func (s *Scheduler) NewTask(id string) *Task {
	t := Task{
		Id:    id,
		Mutex: s.queue.Lock(fmt.Sprintf("task:%s", id)),
	}
	return &t
}

// TaskSchedule is the schedule for when the task is to occur.
type TaskSchedule struct {
	Recurrence string    `json:"recurrence"`
	Count      int       `json:"count"`
	UpdatedAt  time.Time `json:"updatedAt"`
	sync.RWMutex
}
