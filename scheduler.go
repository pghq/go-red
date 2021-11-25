package red

import (
	"context"
	"sync"
	"time"

	"github.com/pghq/go-tea"
	"github.com/teambition/rrule-go"
)

const (
	// DefaultSchedulerInterval is the default period between checking tasks to schedule
	DefaultSchedulerInterval = time.Millisecond

	// DefaultEnqueueTimeout is the default time allowed for queue write ops
	DefaultEnqueueTimeout = 10 * time.Millisecond

	// DefaultDequeueTimeout is the default time allowed for queue read ops
	DefaultDequeueTimeout = 10 * time.Millisecond
)

// Scheduler is an instance of a persistent background scheduler
type Scheduler struct {
	interval       time.Duration
	stop           chan struct{}
	queue          *Red
	enqueueTimeout time.Duration
	dequeueTimeout time.Duration
	lock           sync.RWMutex
	tasks          map[string]*Task
	completed      chan *Task
	wg             sync.WaitGroup
	notify         func(t *Task)
	notifyWorker   func(msg *Message)
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

// Notify is executed after a task has been scheduled, ignored or otherwise errored while attempting to
func (s *Scheduler) Notify(notify func(t *Task)) *Scheduler {
	s.notify = notify

	return s
}

// NotifyWorker is executed after a message has been popped or otherwise errored while attempting to
func (s *Scheduler) NotifyWorker(notify func(msg *Message)) *Scheduler {
	s.notifyWorker = notify

	return s
}

// Start begins scheduling tasks.
func (s *Scheduler) Start() {
	ctx, cancel := context.WithCancel(context.Background())

	s.wg.Add(1)
	go s.start(ctx)
	tea.Log("info", "scheduler: started")

	<-s.stop
	cancel()
	go func() {
		s.wg.Wait()
		s.Stop()
	}()
	<-s.stop
	tea.Log("info", "scheduler: stopped")
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

		s.lock.RLock()
		_, present := s.tasks[task.Id]
		s.lock.RUnlock()
		if present {
			tea.Logf("info", "scheduler: task=%s already in ledger", task.Id)
			continue
		}

		s.lock.Lock()
		s.tasks[task.Id] = task
		s.lock.Unlock()
		tea.Logf("info", "scheduler: task=%s added to ledger", task.Id)
	}

	return s
}

// Worker creates a new worker for handling scheduled tasks.
func (s *Scheduler) Worker(job func(task *Task)) *Worker {
	h := func(ctx context.Context) {
		tea.Logf("debug", "scheduler.worker.job: started")
		for {
			dequeueCtx, cancel := context.WithTimeout(ctx, s.dequeueTimeout)
			msg, err := s.queue.Dequeue(dequeueCtx)
			cancel()
			if err != nil {
				if s.notifyWorker != nil {
					go s.notifyWorker(nil)
				}

				break
			}

			go func() {
				tea.Logf("info", "scheduler.worker.job: item=%s", msg.Id)
				defer func() {
					if err := msg.Ack(ctx); err != nil {
						tea.SendError(err)
					}

					if s.notifyWorker != nil {
						go s.notifyWorker(msg)
					}
				}()

				var task Task
				if err := msg.Decode(&task); err != nil {
					tea.SendError(err)
					return
				}
				job(&task)
				tea.Logf("info", "scheduler.worker.job: task=%s handled", task.Id)
			}()
		}
		tea.Logf("debug", "scheduler.worker.job: finished")
	}

	w := NewWorker(h)
	return w
}

func (s *Scheduler) start(ctx context.Context) {
	defer s.wg.Done()
	go func() {
		for {
			select {
			case <-ctx.Done():
				tea.Log("info", "scheduler: background task #1 stopped")
				return
			case <-time.After(s.interval):
			}

			now := time.Now()
			s.lock.RLock()
			for _, task := range s.tasks {
				if !task.Lock() {
					continue
				}

				if !task.CanSchedule(now) {
					task.Unlock()
					if s.notify != nil {
						go s.notify(task)
					}
					continue
				}

				go func(task *Task) {
					defer func() {
						task.Unlock()
						if s.notify != nil {
							go s.notify(task)
						}
					}()
					ctx, cancel := context.WithTimeout(ctx, s.enqueueTimeout)
					defer cancel()

					if err := s.queue.Enqueue(ctx, task.Id, task); err != nil {
						tea.SendError(err)
						return
					}

					task.MarkScheduled(now)
					if task.IsComplete() {
						s.completed <- task
					}

					tea.Logf("info", "scheduler: task=%s scheduled", task.Id)
				}(task)
			}
			s.lock.RUnlock()
		}
	}()

	go func() {
		for {
			select {
			case <-ctx.Done():
				tea.Log("info", "scheduler: background task #2 stopped")
				return
			case <-time.After(s.interval):
			}

			for removing := true; removing; {
				select {
				case task := <-s.completed:
					s.lock.Lock()
					delete(s.tasks, task.Id)
					s.lock.Unlock()
					tea.Logf("info", "scheduler: task=%s removed from ledger", task.Id)
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
		return tea.BadRequest(err)
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
