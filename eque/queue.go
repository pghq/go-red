package eque

import "context"

// RedQueue is an interface providing FIFO ordering for messages + locking.
type RedQueue interface {
	Dequeue(ctx context.Context) (Message, error)
	Enqueue(ctx context.Context, id string, value interface{}) error
}
