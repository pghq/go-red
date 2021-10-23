package eque

import (
	"context"
	"fmt"

	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

func (q *Red) Dequeue(ctx context.Context) (*Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			m := q.Message()
			if m == nil{
				return nil, errors.NewBadRequest("no messages")
			}

			mutex := q.pool.NewMutex(fmt.Sprintf("eque.r.%s", m.Id), q.readOptions...)
			if err := mutex.LockContext(ctx); err != nil {
				_ = m.Reject(ctx)
				continue
			}


			return m, nil
		}
	}
}
