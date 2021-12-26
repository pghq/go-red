package red

import (
	"context"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-tea"
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
	sync           *redsync.Mutex
	syncTimeout    time.Duration
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
	w, err := s.lock(ctx)
	if err != nil {
		tea.Log(ctx, "error", err)
		cancel()
		return
	}

	s.sync = w
	defer s.sync.UnlockContext(ctx)

	s.wg.Add(1)
	go s.start(ctx)
	go s.worker.Start()

	tea.Log(ctx, "info", "scheduler: started")
	<-s.stop
	cancel()

	go func() {
		s.wg.Wait()
		s.worker.Stop()
		s.Stop()
	}()

	<-s.stop
	tea.Log(ctx, "info", "scheduler: stopped")
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
			tea.Logf(context.Background(), "debug", "scheduler: task=%s already in ledger", task.Id)
			continue
		}

		s.rwlock.Lock()
		s.tasks[task.Id] = task
		s.rwlock.Unlock()
		tea.Logf(context.Background(), "info", "scheduler: task=%s added to ledger", task.Id)
	}

	return s
}

// Handle tasks when scheduled
func (s *Scheduler) Handle(fn func(task *Task)) {
	h := func(ctx context.Context) {
		tea.Logf(ctx, "debug", "scheduler.worker.job: started")
		for {
			ctx, cancel := context.WithTimeout(context.Background(), s.dequeueTimeout)
			msg, err := s.queue.Dequeue(ctx)
			cancel()
			if err != nil {
				break
			}

			go func() {
				tea.Logf(ctx, "info", "scheduler.worker.job: item=%s", msg.Id)
				defer func() {
					if err := msg.Ack(ctx); err != nil {
						tea.Log(ctx, "error", err)
					}
				}()

				var task Task
				if err := msg.Decode(&task); err == nil {
					fn(&task)
					tea.Logf(ctx, "info", "scheduler.worker.job: task=%s handled", task.Id)
				}
			}()
		}
		tea.Logf(ctx, "debug", "scheduler.worker.job: finished")
	}

	s.worker.AddJobs(h)
}

func (s *Scheduler) start(ctx context.Context) {
	defer s.wg.Done()
	go func() {
		for {
			if _, err := s.sync.ExtendContext(ctx); err != nil {
				s.Stop()
				tea.Log(ctx, "error", err)
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				tea.Log(ctx, "info", "scheduler: background task #2 stopped")
				return
			case <-time.After(s.interval):
			}

			now := time.Now()
			s.rwlock.RLock()
			for _, task := range s.tasks {
				if !task.Lock() {
					continue
				}

				if !task.CanSchedule(now) {
					task.Unlock()
					continue
				}

				go func(task *Task) {
					defer func() {
						task.Unlock()
					}()
					ctx, cancel := context.WithTimeout(ctx, s.enqueueTimeout)
					defer cancel()

					if err := s.queue.Enqueue(ctx, task.Id, task); err != nil {
						tea.Log(ctx, "error", err)
						return
					}

					task.MarkScheduled(now)
					if task.IsComplete() {
						s.completed <- task
					}

					tea.Logf(ctx, "info", "scheduler: task=%s scheduled", task.Id)
				}(task)
			}
			s.rwlock.RUnlock()
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				tea.Log(ctx, "info", "scheduler: background task #3 stopped")
				return
			case <-time.After(s.interval):
			}

			for removing := true; removing; {
				select {
				case task := <-s.completed:
					s.rwlock.Lock()
					delete(s.tasks, task.Id)
					s.rwlock.Unlock()
					tea.Logf(ctx, "info", "scheduler: task=%s removed from ledger", task.Id)
				default:
					removing = false
				}
			}
		}
	}()

	<-ctx.Done()
}

func (s *Scheduler) lock(ctx context.Context) (*redsync.Mutex, error) {
	ctx, cancel := context.WithTimeout(ctx, s.syncTimeout)
	defer cancel()
	w := s.queue.Lock("scheduler", redsync.WithExpiry(s.interval+time.Second), redsync.WithTries(13))
	if err := w.LockContext(ctx); err != nil {
		return nil, tea.Stack(err)
	}
	return w, nil
}

// NewScheduler creates a scheduler instance.
func NewScheduler(queue *Red) *Scheduler {
	s := Scheduler{
		worker:         NewWorker(),
		queue:          queue,
		interval:       DefaultSchedulerInterval,
		enqueueTimeout: DefaultEnqueueTimeout,
		dequeueTimeout: DefaultDequeueTimeout,
		syncTimeout:    5 * time.Minute,
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
	lock     chan struct{}
}

// Lock notifies the scheduler to ignore scheduling.
func (t *Task) Lock() bool {
	select {
	case <-t.lock:
		return true
	default:
		return false
	}
}

// Unlock notifies the scheduler that the task has been scheduled.
func (t *Task) Unlock() {
	select {
	case t.lock <- struct{}{}:
	default:
	}
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
		return tea.AsErrBadRequest(err)
	}

	t.Schedule.Recurrence = rfc

	return nil
}

// NewTask creates a new instance of a task to be scheduled.
func NewTask(id string) *Task {
	t := Task{
		Id:   id,
		lock: make(chan struct{}, 1),
	}

	t.Unlock()
	return &t
}

// TaskSchedule is the schedule for when the task is to occur.
type TaskSchedule struct {
	Recurrence string    `json:"recurrence"`
	Count      int       `json:"count"`
	UpdatedAt  time.Time `json:"updatedAt"`
	sync.RWMutex
}
