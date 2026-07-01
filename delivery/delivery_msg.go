package delivery

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

type DM struct {
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

func NewDM() *DM {
	return &DM{
		ID:           uuid.New().String(),
		Headers:      make(map[string]interface{}),
		Timestamp:    time.Now(),
		Priority:     0,
		DeliveryMode: Persistent,
		Context:      Context{},
	}
}

func (m *DM) IncrRetryCount() {
	m.Context.RetryCount++
}

func (m *DM) GetRetryCount() int {
	return m.Context.RetryCount
}
