package mq

import (
	"fmt"
)

type mq struct {
	cfg        MqConfig
	connection Connection
	chanPool   ChannelPool
}

type MQ interface {
	Subscriber(cfg SubscriberConfig) Subscriber
	Publisher(cfg PublisherConfig) Publisher
	Close() error
}

func NewMQ(cfg MqConfig) (MQ, error) {
	connection := NewConnection(cfg.Connection)

	if err := connection.Connect(); err != nil {
		return nil, err
	}

	chanPool, err := NewChannelPool(cfg.Pool, connection)
	if err != nil {
		return nil, err
	}

	return &mq{
		cfg:        cfg,
		connection: connection,
		chanPool:   chanPool,
	}, nil
}

func (m *mq) Subscriber(cfg SubscriberConfig) Subscriber {
	return NewSubscriber(cfg, m.connection)
}

func (m *mq) Publisher(cfg PublisherConfig) Publisher {
	return NewPublisher(cfg, m.chanPool)
}

func (m *mq) Close() error {
	var mErr []error

	if err := m.chanPool.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("channel pool close error: %w", err))
	}

	if err := m.connection.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("connection close error: %w", err))
	}

	if len(mErr) > 0 {
		return fmt.Errorf("errors closing mq: %v", mErr)
	}

	return nil
}
