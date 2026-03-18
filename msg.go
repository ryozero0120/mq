package mq

import (
	"github.com/google/uuid"
)

type Msg struct {
	ID      string
	Headers map[string]interface{}
	Body    []byte
}

func NewMsg() *Msg {
	return &Msg{
		ID:      uuid.NewString(),
		Headers: make(map[string]interface{}),
	}
}
