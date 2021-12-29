package red

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewQWorker(t *testing.T) {
	t.Parallel()

	t.Run("can create instance", func(t *testing.T) {
		w := NewQWorker("test")
		assert.NotNil(t, w)
		w.Stop()
		w.Stop()
	})
}

func TestWorker_Every(t *testing.T) {
	t.Parallel()

	t.Run("sets a new value", func(t *testing.T) {
		w := NewWorker("test").Every(time.Second)
		assert.NotNil(t, w)
		assert.Equal(t, w.interval, time.Second)
	})
}

func TestWorker_Concurrent(t *testing.T) {
	t.Parallel()

	t.Run("sets a new value", func(t *testing.T) {
		w := NewWorker("test").Concurrent(5)
		assert.NotNil(t, w)
		assert.Equal(t, w.instances, 5)
	})
}

func TestWorker_Start(t *testing.T) {
	t.Parallel()

	t.Run("can run", func(t *testing.T) {
		done := make(chan struct{}, 2)
		job := func() {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		w := NewWorker("test", job)
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
		job := func() {
			select {
			case done <- struct{}{}:
				panic("an error has occurred")
			default:
			}
		}

		w := NewWorker("test", job)
		go w.Start()
		defer w.Stop()
		<-done
	})

	t.Run("handles cancelled jobs", func(t *testing.T) {
		done := make(chan struct{}, 1)
		job := func() {
			done <- struct{}{}
		}

		w := NewWorker("test", job)
		go w.Start()
		defer w.Stop()

		<-done
	})
}
