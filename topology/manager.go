package topology

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/observability"
)

type ChannelProvider interface {
	Acquire(ctx context.Context) (*amqp.Channel, error)
	Release(ch *amqp.Channel) error
}

type TopologyManager interface {
	DeclareExchange(ctx context.Context, config ExchangeConfig) error
	DeclareQueue(ctx context.Context, config QueueConfig) (amqp.Queue, error)
	CreateBinding(ctx context.Context, config BindingConfig) error
}

type topologyManager struct {
	logger      observability.Logger
	channelPool ChannelProvider
}

func NewTopologyManager(logger observability.Logger, channelPool ChannelProvider) TopologyManager {
	return &topologyManager{
		logger:      logger,
		channelPool: channelPool,
	}
}

func (m *topologyManager) DeclareExchange(ctx context.Context, config ExchangeConfig) error {
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

func (m *topologyManager) DeclareQueue(ctx context.Context, config QueueConfig) (amqp.Queue, error) {
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

func (m *topologyManager) CreateBinding(ctx context.Context, config BindingConfig) error {
	binding := NewBinding(
		config,
		m.channelPool,
	)

	return binding.Create(ctx)
}
