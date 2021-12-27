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
	jobs      []Job
	wg        sync.WaitGroup
	log       Log
}

// AddJobs adds jobs to the worker
func (w *Worker) AddJobs(jobs ...Job) {
	w.jobs = append(w.jobs, jobs...)
}

// Start begins processing tasks.
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < w.instances; i++ {
		w.wg.Add(1)
		go w.start(ctx, i+1)
	}

	w.log.Logf("info", "worker: workers=%d, started", w.instances)

	<-w.stop
	cancel()
	go func() {
		w.wg.Wait()
		w.Stop()
	}()
	<-w.stop
	w.log.Log("info", "worker: stopped")
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
				go func(i int, job Job) {
					defer wg.Done()
					defer func() {
						if err := recover(); err != nil {
							tea.Log(ctx, "error", err)
						}
					}()

					w.log.Logf("debug", "worker: instance=%d, job=%d, started", instance, i)
					ctx, cancel := context.WithTimeout(ctx, w.interval)
					defer cancel()
					job(ctx)
					w.log.Logf("debug", "worker: instance=%d, job=%d, finished", instance, i)
				}(i, job)
			}

			wg.Wait()
		}
	}()

	w.log.Logf("info", "worker: instance=%d, started", instance)
	<-ctx.Done()
	w.log.Logf("info", "worker: instance=%d, stopped", instance)
}

// NewWorker creates a new worker instance.
func NewWorker(jobs ...Job) *Worker {
	w := Worker{
		instances: DefaultInstances,
		interval:  DefaultWorkerInterval,
		jobs:      jobs,
		stop:      make(chan struct{}, 1),
	}

	return &w
}

// NewQWorker creates a new worker instance with quiet mode enabled.
func NewQWorker(jobs ...Job) *Worker {
	w := NewWorker(jobs...)
	w.log.quiet = true
	return w
}

// Job is a task to be executed.
type Job func(ctx context.Context)

// Log instance with quiet support
type Log struct{ quiet bool }

// Log value
func (l Log) Log(level string, v ...interface{}) {
	if !l.quiet {
		tea.Log(context.Background(), level, v...)
	}
}

// Logf formatted value
func (l Log) Logf(level, format string, args ...interface{}) {
	if !l.quiet {
		tea.Logf(context.Background(), level, format, args...)
	}
}
