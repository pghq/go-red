package red

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-tea"
)

func (r *Red) Enqueue(ctx context.Context, id string, value interface{}) error {
	mutex := r.pool.NewMutex(fmt.Sprintf("red.w.%s", id), r.writeOptions...)
	if err := mutex.LockContext(ctx); err != nil {
		if tea.IsError(err, redsync.ErrFailed) {
			return tea.BadRequest(err)
		}

		return tea.Error(err)
	}

	err := func() error {
		v, err := json.Marshal(value)
		if err != nil {
			return tea.NewBadRequest(err)
		}

		payload, _ := json.Marshal(&Message{Id: id, Value: v})
		if err := r.queue.PublishBytes(payload); err != nil {
			return err
		}

		return nil
	}()

	if err != nil {
		_, _ = mutex.UnlockContext(ctx)
		return tea.Error(err)
	}

	return nil
}
