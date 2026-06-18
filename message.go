package mq

import (
	"time"

	"github.com/google/uuid"
)

type DeliveryMode uint8

const (
	// Transient messages are not persisted to disk
	Transient DeliveryMode = 1
	// Persistent messages are persisted to disk
	Persistent DeliveryMode = 2
)

type Context struct {
	TraceID    string
	SpanID     string
	RetryCount int
}

type Message struct {
	ID              string
	Headers         map[string]interface{}
	Body            []byte
	ContentType     string
	ContentEncoding string
	CorrelationID   string
	ReplyTo         string
	Timestamp       time.Time
	Priority        uint8
	DeliveryMode    DeliveryMode
	Context         Context
}

func NewMessage() *Message {
	return &Message{
		ID:           uuid.New().String(),
		Headers:      make(map[string]interface{}),
		Timestamp:    time.Now(),
		Priority:     0,
		DeliveryMode: Persistent,
		Context:      Context{},
	}
}
