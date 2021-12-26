package red

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewQWorker(t *testing.T) {
	t.Parallel()

	t.Run("can create instance", func(t *testing.T) {
		w := NewQWorker()
		assert.NotNil(t, w)
		w.Stop()
		w.Stop()
	})
}

func TestWorker_Every(t *testing.T) {
	t.Parallel()

	t.Run("sets a new value", func(t *testing.T) {
		w := NewWorker().Every(time.Second)
		assert.NotNil(t, w)
		assert.Equal(t, w.interval, time.Second)
	})
}

func TestWorker_Concurrent(t *testing.T) {
	t.Parallel()

	t.Run("sets a new value", func(t *testing.T) {
		w := NewWorker().Concurrent(5)
		assert.NotNil(t, w)
		assert.Equal(t, w.instances, 5)
	})
}

func TestWorker_Start(t *testing.T) {
	t.Parallel()

	t.Run("can run", func(t *testing.T) {
		done := make(chan struct{}, 2)
		job := func(ctx context.Context) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		w := NewWorker(job)
		go w.Start()
		defer w.Stop()
		<-done
	})

	t.Run("handles panics", func(t *testing.T) {
		defer func() {
			if err := recover(); err != nil {
				t.Fatalf("panic not expected: %+v", err)
			}
		}()

		done := make(chan struct{}, 2)
		job := func(ctx context.Context) {
			select {
			case done <- struct{}{}:
				panic("an error has occurred")
			default:
			}
		}

		w := NewWorker(job)
		go w.Start()
		defer w.Stop()
		<-done
	})

	t.Run("handles cancelled jobs", func(t *testing.T) {
		done := make(chan struct{}, 1)
		job := func(ctx context.Context) {
			done <- struct{}{}
		}

		w := NewWorker(job)
		go w.Start()
		defer w.Stop()

		<-done
	})
}
