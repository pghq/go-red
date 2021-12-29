package red

import (
	"context"
	"sync"
	"time"

	"github.com/pghq/go-tea"
)

const (
	// DefaultInstances is the default number of simultaneous workers
	DefaultInstances = 1

	// DefaultWorkerInterval is the default period between running batches of jobs
	DefaultWorkerInterval = time.Millisecond
)

// Worker is an instance of a background worker.
type Worker struct {
	instances int
	interval  time.Duration
	stop      chan struct{}
	jobs      []func()
	wg        sync.WaitGroup
	log       Log
	name      string
}

// AddJobs adds jobs to the worker
func (w *Worker) AddJobs(jobs ...func()) {
	w.jobs = append(w.jobs, jobs...)
}

// Start begins processing tasks.
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < w.instances; i++ {
		w.wg.Add(1)
		go w.start(ctx, i+1)
	}

	w.log.Logf("info", "%s.worker: workers=%d, started", w.name, w.instances)

	<-w.stop
	cancel()
	go func() {
		w.wg.Wait()
		w.Stop()
	}()
	<-w.stop
	w.log.Logf("info", "%s.worker: stopped", w.name)
}

// Concurrent sets the number of simultaneous instances to process tasks.
func (w *Worker) Concurrent(instances int) *Worker {
	w.instances = instances

	return w
}

// Every sets the time between processing tasks.
func (w *Worker) Every(interval time.Duration) *Worker {
	w.interval = interval

	return w
}

// Stop stops the worker and waits for all instances to terminate.
func (w *Worker) Stop() {
	select {
	case w.stop <- struct{}{}:
	default:
	}
}

func (w *Worker) start(ctx context.Context, instance int) {
	defer w.wg.Done()
	go func() {
		wg := sync.WaitGroup{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.interval):
			}

			for i, job := range w.jobs {
				wg.Add(1)
				go func(i int, job func()) {
					defer wg.Done()
					w.log.Logf("debug", "%s.worker: instance=%d, job=%d, started", w.name, instance, i)
					go func() {
						defer func() {
							if err := recover(); err != nil {
								tea.Log(ctx, "error", err)
							}
						}()
						job()
					}()
					w.log.Logf("debug", "%s.worker: instance=%d, job=%d, finished", w.name, instance, i)
				}(i, job)
			}

			wg.Wait()
		}
	}()

	w.log.Logf("info", "%s.worker: instance=%d, started", w.name, instance)
	<-ctx.Done()
	w.log.Logf("info", "%s.worker: instance=%d, stopped", w.name, instance)
}

// NewWorker creates a new worker instance.
func NewWorker(name string, jobs ...func()) *Worker {
	w := Worker{
		instances: DefaultInstances,
		interval:  DefaultWorkerInterval,
		jobs:      jobs,
		stop:      make(chan struct{}, 1),
		name:      name,
	}
	return &w
}

// NewQWorker creates a new worker instance with quiet mode enabled.
func NewQWorker(name string, jobs ...func()) *Worker {
	w := NewWorker(name, jobs...)
	w.log.quiet = true
	return w
}

// Log instance with quiet support
type Log struct{ quiet bool }

// Logf formatted value
func (l Log) Logf(level, format string, args ...interface{}) {
	if !l.quiet {
		tea.Logf(context.Background(), level, format, args...)
	}
}
