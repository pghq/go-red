package eque

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/go-redsync/redsync/v4"
)

var (
	// ErrAcquireLockFailed is raised when locks can not be acquired
	ErrAcquireLockFailed = errors.New("failed to acquire lock")
)

func (q *eque) Enqueue(ctx context.Context, id string, value interface{}) error {
	mutex := q.locks.NewWMutex(id)
	if err := mutex.LockContext(ctx); err != nil {
		if err == redsync.ErrFailed{
			return ErrAcquireLockFailed
		}
		return err
	}

	err := func() error{
		v, err := json.Marshal(value)
		if err != nil {
			return err
		}

		payload, err := json.Marshal(&message{
			Key:  id,
			Value: v,
		})

		if err != nil {
			return err
		}

		if err := q.queue.PublishBytes(payload); err != nil {
			return err
		}

		return nil
	}()

	if err != nil{
		_, _ = mutex.UnlockContext(ctx)
		return err
	}

	return nil
}
