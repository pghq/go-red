package red

import (
	"context"

	"github.com/pghq/go-tea"
)

// Dequeue message from the queue
func (r *Red) Dequeue(ctx context.Context) (*Message, error) {
	if err := r.Error(); err != nil {
		return nil, tea.Stack(err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			m := r.message()
			if m == nil {
				return nil, tea.ErrBadRequest("no messages")
			}
			mutex := r.RLock(m.Id)
			if err := mutex.LockContext(ctx); err != nil {
				_ = m.Reject(ctx)
				continue
			}

			return m, nil
		}
	}
}
