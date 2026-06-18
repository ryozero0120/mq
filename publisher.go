package mq

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublisherConfig struct {
	Exchange   string
	RoutingKey string
}

type publisher struct {
	channelPool ChannelPool
	config      PublisherConfig
	middlewares []PublisherMiddleware

	mu sync.RWMutex
}

type Publisher interface {
	Publish(ctx context.Context, msg *Message) error
	Use(middleware PublisherMiddleware)
	Close() error
}

type PublisherFunc func(ctx context.Context, msg *Message) error

type PublisherMiddleware interface {
	Intercept(ctx context.Context, msg *Message, next PublisherFunc) error
}

func NewPublisher(config PublisherConfig, channelPool ChannelPool) Publisher {
	return &publisher{
		config:      config,
		channelPool: channelPool,
	}
}

func (p *publisher) Publish(ctx context.Context, msg *Message) error {
	var err error
	if len(p.middlewares) > 0 {
		err = p.applyMiddleware(ctx, msg, 0, p.publish)
	} else {
		err = p.publish(ctx, msg)
	}

	return err
}

func (p *publisher) Use(mw PublisherMiddleware) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.middlewares = append(p.middlewares, mw)
}

func (p *publisher) Close() error {
	return nil
}

func (p *publisher) applyMiddleware(ctx context.Context, msg *Message, index int, next PublisherFunc) error {
	if index >= len(p.middlewares) {
		return next(ctx, msg)
	}

	middleware := p.middlewares[index]
	return middleware.Intercept(ctx, msg, func(ctx context.Context, msg *Message) error {
		return p.applyMiddleware(ctx, msg, index+1, next)
	})
}

func (p *publisher) publish(ctx context.Context, msg *Message) error {
	ch, err := p.channelPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer p.channelPool.Release(ch)

	publishing := amqp.Publishing{
		Headers:         amqp.Table(msg.Headers),
		ContentType:     msg.ContentType,
		ContentEncoding: msg.ContentEncoding,
		DeliveryMode:    uint8(msg.DeliveryMode),
		Priority:        msg.Priority,
		CorrelationId:   msg.CorrelationID,
		ReplyTo:         msg.ReplyTo,
		MessageId:       msg.ID,
		Timestamp:       msg.Timestamp,
		Body:            msg.Body,
	}

	publishCtx := ctx
	err = ch.PublishWithContext(
		publishCtx,
		p.config.Exchange,
		p.config.RoutingKey,
		true,
		false,
		publishing,
	)
	if err != nil {
		// retry policy

		return err
	}

	return nil
}
