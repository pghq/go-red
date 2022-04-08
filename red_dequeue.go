package red

import (
	"context"

	"github.com/pghq/go-tea/trail"
)

var (
	// ErrNoMessages is an error returned when a request is made but no messages are available
	ErrNoMessages = trail.NewErrorBadRequest("no messages currently available")
)

// Dequeue message from the queue
func (r *Red) Dequeue(ctx context.Context) (*Message, error) {
	if err := r.Error(); err != nil {
		return nil, trail.Stacktrace(err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			m := r.message()
			if m == nil {
				return nil, ErrNoMessages
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
