package topology

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
)

// BindingConfig describes a queue-to-exchange binding.
type BindingConfig struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  map[string]interface{}
}

type Binding struct {
	config      BindingConfig
	channelPool channel.ChannelPool
	declared    atomic.Bool
	mu          sync.Mutex
}

func NewBinding(config BindingConfig, channelPool channel.ChannelPool) *Binding {
	return &Binding{
		config:      config,
		channelPool: channelPool,
	}
}

func (b *Binding) Create(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	channel, err := b.channelPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire channel: %w", err)
	}

	defer b.channelPool.Release(channel)

	err = channel.QueueBind(
		b.config.Queue,
		b.config.RoutingKey,
		b.config.Exchange,
		b.config.NoWait,
		amqp.Table(b.config.Arguments),
	)
	if err != nil {
		return fmt.Errorf("bind queue %q to exchange %q: %w", b.config.Queue, b.config.Exchange, err)
	}

	return nil
}

func (e *Binding) IsDeclared() bool {
	return e.declared.Load()
}
