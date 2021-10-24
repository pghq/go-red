// Copyright 2021 PGHQ. All Rights Reserved.
//
// Licensed under the GNU General Public License, Version 3 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package worker provides a background worker for offline processing.
package worker

import (
	"context"
	"sync"
	"time"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
	"github.com/pghq/go-museum/museum/diagnostic/log"
)

const (
	// DefaultInstances is the default number of simultaneous workers
	DefaultInstances = 1

	// DefaultInterval is the default period between running batches of jobs
	DefaultInterval = time.Millisecond
)

// Worker is an instance of a background worker.
type Worker struct {
	instances int
	interval  time.Duration
	stop      chan struct{}
	jobs      []Job
	wg        sync.WaitGroup
}

// Start begins processing tasks.
func (w *Worker) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < w.instances; i++ {
		w.wg.Add(1)
		go w.start(ctx, i+1)
	}

	log.Infof("worker: workers=%d, started", w.instances)

	<-w.stop
	cancel()
	go func() {
		w.wg.Wait()
		w.Stop()
	}()
	<-w.stop
	log.Info("worker: stopped")
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
	stopped := make(chan struct{})
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
							errors.Recover(err)
						}
					}()

					log.Debugf("worker: instance=%d, job=%d, started", instance, i)
					ctx, cancel := context.WithTimeout(ctx, w.interval)
					defer cancel()
					job(ctx)
					log.Debugf("worker: instance=%d, job=%d, finished", instance, i)
				}(i, job)
			}

			go func() {
				wg.Wait()
				stopped <- struct{}{}
			}()

			select {
			case <-stopped:
			case <-ctx.Done():
			}
		}
	}()

	log.Infof("worker: instance=%d, started", instance)
	<-ctx.Done()
	log.Infof("worker: instance=%d, stopped", instance)
}

// New creates a new worker instance.
func New(jobs ...Job) *Worker {
	w := Worker{
		instances: DefaultInstances,
		interval:  DefaultInterval,
		jobs:      jobs,
		stop:      make(chan struct{}, 1),
	}

	return &w
}

// Job is a task to be executed.
type Job func(ctx context.Context)
