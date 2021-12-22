package red

import (
	"context"
	"fmt"

	"github.com/pghq/go-tea"
)

func (r *Red) Dequeue(ctx context.Context) (*Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			m := r.Message()
			if m == nil {
				return nil, tea.ErrBadRequest("no messages")
			}

			mutex := r.pool.NewMutex(fmt.Sprintf("red.r.%s", m.Id), r.readOptions...)
			if err := mutex.LockContext(ctx); err != nil {
				_ = m.Reject(ctx)
				continue
			}

			return m, nil
		}
	}
}
