package reliability

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/delivery"
)

type DLQExecutor interface {
	Handle(ctx context.Context, m *delivery.DM) error
	PublishToDLQ(ctx context.Context, m *delivery.DM) error
}

type DLQConfig struct {
	Exchange   string
	Queue      string
	RoutingKey string
	TTL        time.Duration
	MaxRetries int
}

type DLQHandler struct {
	channelPool channel.ChannelPool
	config      DLQConfig
}

func NewDLQHandler(channelPool channel.ChannelPool, config DLQConfig) *DLQHandler {
	return &DLQHandler{
		channelPool: channelPool,
		config:      config,
	}
}

func (d *DLQHandler) Handle(ctx context.Context, m *delivery.DM) error {
	// Publish to DLQ
	if err := d.PublishToDLQ(ctx, m); err != nil {
		return fmt.Errorf("failed to publish to DLQ: %w", err)
	}

	return nil
}

func (d *DLQHandler) PublishToDLQ(ctx context.Context, m *delivery.DM) error {
	ch, err := d.channelPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer d.channelPool.Release(ch)

	publishing := amqp.Publishing{
		Headers:         amqp.Table(m.Headers),
		ContentType:     m.ContentType,
		ContentEncoding: m.ContentEncoding,
		DeliveryMode:    uint8(m.DeliveryMode),
		Priority:        m.Priority,
		CorrelationId:   m.CorrelationID,
		ReplyTo:         m.ReplyTo,
		MessageId:       m.ID,
		Timestamp:       m.Timestamp,
		Body:            m.Body,
	}

	publishCtx := ctx
	err = ch.PublishWithContext(
		publishCtx,
		d.config.Exchange,
		d.config.RoutingKey,
		false, // mandatory
		false, // immediate
		publishing,
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to DLQ: %w", err)
	}

	return nil
}
