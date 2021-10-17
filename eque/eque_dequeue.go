package eque

import (
	"context"
	"errors"
)

var (
	// ErrNoMessages is raised when there are no messages in the queue
	ErrNoMessages = errors.New("no messages")
)

func (q *eque) Dequeue(ctx context.Context) (Message, error) {
	for {
		select {
		case message := <-q.messages:
			mutex := q.locks.NewRMutex(message.Key)
			if err := mutex.LockContext(ctx); err != nil {
				_ = message.Reject(ctx)
				continue
			}
			return message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, ErrNoMessages
		}
	}
}
