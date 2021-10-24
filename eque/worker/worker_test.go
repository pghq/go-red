package worker

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/pghq/go-museum/museum/diagnostic/log"
)

func TestMain(m *testing.M) {
	log.Writer(io.Discard)
	code := m.Run()
	os.Exit(code)
}

func TestNew(t *testing.T) {
	t.Run("can create instance", func(t *testing.T) {
		w := New()
		assert.NotNil(t, w)
	})
}

func TestWorker_Every(t *testing.T) {
	t.Run("sets a new value", func(t *testing.T) {
		w := New().Every(time.Second)
		assert.NotNil(t, w)
		assert.Equal(t, w.interval, time.Second)
	})
}

func TestWorker_Concurrent(t *testing.T) {
	t.Run("sets a new value", func(t *testing.T) {
		w := New().Concurrent(5)
		assert.NotNil(t, w)
		assert.Equal(t, w.instances, 5)
	})
}

func TestWorker_Start(t *testing.T) {
	t.Run("can run", func(t *testing.T) {
		done := make(chan struct{}, 2)
		job := func(ctx context.Context) {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		w := New(job)
		go w.Start()
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

		w := New(job)
		defer w.Stop()
		go w.Start()
		<-done
	})

	t.Run("handles cancelled jobs", func(t *testing.T) {
		done := make(chan struct{}, 1)
		job := func(ctx context.Context) {
			done <- struct{}{}
		}

		w := New(job)
		defer w.Stop()
		go w.Start()

		<-done
	})
}
