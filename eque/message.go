package eque

import (
	"context"
	"encoding/json"
)

// Message is an interface describing a message within the queue.
type Message interface {
	// Id gets the message key
	Id() string

	// Ack notifies upstream of successful message handling
	Ack(ctx context.Context) error

	// Reject notifies upstream of unsuccessful message handling
	Reject(ctx context.Context) error

	// Decode fills the supplied interface with the underlying message value
	Decode(v interface{}) error
}

// message is a single instance of a message within the queue.
type message struct {
	Key  string `json:"key"`
	Value []byte `json:"value"`

	locks locks
	ack func() error
	reject func() error
}

func (m *message) Decode(v interface{}) error {
	return json.Unmarshal(m.Value, v)
}

func (m *message) Ack(ctx context.Context) error {
	if m.ack != nil{
		if err := m.ack(); err != nil{
			return err
		}
	}

	w := m.locks.NewWMutex(m.Key)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.locks.NewRMutex(m.Key)
	if _, err := r.UnlockContext(ctx); err != nil {
		return err
	}

	return nil
}

func (m *message) Reject(ctx context.Context) error {
	if m.reject != nil{
		if err := m.reject(); err != nil{
			return err
		}
	}

	w := m.locks.NewWMutex(m.Key)
	if _, err := w.UnlockContext(ctx); err != nil {
		return err
	}

	r := m.locks.NewRMutex(m.Key)
	if _, err := r.UnlockContext(ctx); err != nil {
		return err
	}

	return nil
}

func (m *message) Id() string {
	return m.Key
}