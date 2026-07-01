package topology

import (
	"context"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/observability"
)

type TopologyInstaller interface {
	DeclareExchange(ctx context.Context, config ExchangeConfig) error
	DeclareQueue(ctx context.Context, config QueueConfig) (amqp.Queue, error)
	CreateBinding(ctx context.Context, config BindingConfig) error
	IsExchangeDeclared(name string) bool
	IsQueueDeclared(name string) bool
	IsBindingCreated(config BindingConfig) bool
}

type topology struct {
	logger      observability.Logger
	channelPool channel.ChannelPool

	mu sync.RWMutex

	exchanges map[string]*Exchange
	queues    map[string]*Queue
	bindings  map[string]*Binding
}

func New(logger observability.Logger, channelPool channel.ChannelPool) TopologyInstaller {
	return &topology{
		logger:      logger,
		channelPool: channelPool,
		exchanges:   make(map[string]*Exchange),
		queues:      make(map[string]*Queue),
		bindings:    make(map[string]*Binding),
	}
}

func (m *topology) DeclareExchange(ctx context.Context, config ExchangeConfig) error {
	exchange := NewExchange(
		config,
		m.channelPool,
	)

	if err := exchange.Declare(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	m.exchanges[config.Name] = exchange
	m.mu.Unlock()

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

	m.mu.Lock()
	m.queues[config.Name] = queue
	m.mu.Unlock()

	return q, nil
}

func (m *topology) CreateBinding(ctx context.Context, config BindingConfig) error {
	binding := NewBinding(
		config,
		m.channelPool,
	)

	if err := binding.Create(ctx); err != nil {
		return err
	}

	m.mu.Lock()
	m.bindings[bindingKey(config)] = binding
	m.mu.Unlock()

	return nil
}

func (m *topology) IsExchangeDeclared(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exchange, ok := m.exchanges[name]
	if !ok {
		return false
	}

	return exchange.IsDeclared()
}

func (m *topology) IsQueueDeclared(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	queue, ok := m.queues[name]
	if !ok {
		return false
	}

	return queue.IsDeclared()
}

func (m *topology) IsBindingCreated(config BindingConfig) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	binding, ok := m.bindings[bindingKey(config)]
	if !ok {
		return false
	}

	return binding.IsDeclared()
}

// bindingKey tạo khóa duy nhất cho một binding (exchange + queue + routing key).
func bindingKey(c BindingConfig) string {
	return c.Exchange + "\x00" + c.Queue + "\x00" + c.RoutingKey
}
