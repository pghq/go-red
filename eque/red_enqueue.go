package eque

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-museum/museum/diagnostic/errors"
)

func (q *Red) Enqueue(ctx context.Context, id string, value interface{}) error {
	mutex := q.pool.NewMutex(fmt.Sprintf("eque.w.%s", id), q.writeOptions...)
	if err := mutex.LockContext(ctx); err != nil {
		if errors.Is(err, redsync.ErrFailed) {
			return errors.BadRequest(err)
		}

		return errors.Wrap(err)
	}

	err := func() error {
		v, err := json.Marshal(value)
		if err != nil {
			return errors.NewBadRequest(err)
		}

		payload, _ := json.Marshal(&Message{Id: id, Value: v})
		if err := q.queue.PublishBytes(payload); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		_, _ = mutex.UnlockContext(ctx)
		return errors.Wrap(err)
	}

	return nil
}
