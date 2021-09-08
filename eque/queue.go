package eque

import "context"

// Queue is an interface providing FIFO ordering for messages.
type Queue interface {
	Dequeue(ctx context.Context) (Message, error)
	Enqueue(ctx context.Context, id string, value interface{}) error
}
