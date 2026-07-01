package topology

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/observability"
)

type TopologyInstaller interface {
	DeclareExchange(ctx context.Context, config ExchangeConfig) error
	DeclareQueue(ctx context.Context, config QueueConfig) (amqp.Queue, error)
	CreateBinding(ctx context.Context, config BindingConfig) error
}

type topology struct {
	logger      observability.Logger
	channelPool channel.ChannelPool
}

func New(logger observability.Logger, channelPool channel.ChannelPool) TopologyInstaller {
	return &topology{
		logger:      logger,
		channelPool: channelPool,
	}
}

func (m *topology) DeclareExchange(ctx context.Context, config ExchangeConfig) error {
	exchange := NewExchange(
		config,
		m.channelPool,
	)
	err := exchange.Declare(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (m *topology) DeclareQueue(ctx context.Context, config QueueConfig) (amqp.Queue, error) {
	queue := NewQueue(
		config,
		m.channelPool,
	)
	q, err := queue.Declare(ctx)
	if err != nil {
		return q, err
	}
	return q, nil
}

func (m *topology) CreateBinding(ctx context.Context, config BindingConfig) error {
	binding := NewBinding(
		config,
		m.channelPool,
	)

	return binding.Create(ctx)
}
