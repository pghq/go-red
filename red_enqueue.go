package red

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-redsync/redsync/v4"
	"github.com/pghq/go-tea/trail"
)

// Enqueue message into queue
func (r *Red) Enqueue(ctx context.Context, key, value interface{}) error {
	if err := r.Error(); err != nil {
		return trail.Stacktrace(err)
	}

	id := fmt.Sprintf("%s", key)
	mutex := r.Lock(id)
	if err := mutex.LockContext(ctx); err != nil {
		if trail.IsError(err, redsync.ErrFailed) {
			err = trail.ErrorBadRequest(err)
		}
		return trail.Stacktrace(err)
	}

	err := func() error {
		v, err := json.Marshal(value)
		if err != nil {
			return trail.ErrorBadRequest(err)
		}

		payload, _ := json.Marshal(&Message{Id: id, Value: v})
		return r.queue.PublishBytes(payload)
	}()

	if err != nil {
		_, _ = mutex.UnlockContext(ctx)
		return trail.Stacktrace(err)
	}

	return nil
}
