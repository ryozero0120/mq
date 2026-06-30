package reliability

import (
	"context"

	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/delivery"
)

type DLQExecutor interface {
	Handle(ctx context.Context, m delivery.Message) error
	PublishToDLQ(ctx context.Context, m delivery.Message) error
}

type DQLConfig struct {
	channelPool channel.ChannelPool
}

type DLQHandler struct {
}

func NewDLQHandler() *DLQHandler {
	return &DLQHandler{}
}

func (d *DLQHandler) Handle(ctx context.Context, m delivery.Message) error {
	return nil
}

func (d *DLQHandler) PublishToDLQ(ctx context.Context, m delivery.Message) error {
	return nil
}
