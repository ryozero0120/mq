package mq

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PublisherConfig struct {
	Exchange   string
	RoutingKey string
}

type publisher struct {
	cfg         PublisherConfig
	channelPool ChannelPool
}

type Publisher interface {
	Publish(ctx context.Context, m Msg) error
	Close() error
}

func NewPublisher(cfg PublisherConfig, pool ChannelPool) Publisher {
	return &publisher{
		cfg:         cfg,
		channelPool: pool,
	}
}

func (p *publisher) Publish(ctx context.Context, m Msg) error {
	ch, err := p.channelPool.Acquire(ctx)
	if err != nil {
		return err
	}

	mp := amqp.Publishing{
		Headers: amqp.Table(m.Headers),
		Body:    m.Body,
	}

	err = ch.PublishWithContext(ctx, p.cfg.Exchange, p.cfg.RoutingKey, true, false, mp)
	if err != nil {
		return err
	}

	return nil
}

func (p *publisher) Close() error {
	return nil
}
