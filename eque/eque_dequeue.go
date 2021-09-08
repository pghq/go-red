package eque

import (
	"context"
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
			return nil, nil
		}
	}
}
