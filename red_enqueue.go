package red

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-tea"
)

// Enqueue message into queue
func (r *Red) Enqueue(ctx context.Context, key, value interface{}) error {
	if err := r.Error(); err != nil {
		return tea.Stack(err)
	}

	id := fmt.Sprintf("%s", key)
	mutex := r.Lock(id)
	if err := mutex.LockContext(ctx); err != nil {
		if tea.IsError(err, redsync.ErrFailed) {
			err = tea.AsErrBadRequest(err)
		}
		return tea.Stack(err)
	}

	err := func() error {
		v, err := json.Marshal(value)
		if err != nil {
			return tea.ErrBadRequest(err)
		}

		payload, _ := json.Marshal(&Message{Id: id, Value: v})
		return r.queue.PublishBytes(payload)
	}()

	if err != nil {
		_, _ = mutex.UnlockContext(ctx)
		return tea.Stack(err)
	}

	return nil
}
