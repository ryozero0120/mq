package topology

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
)

// Exchange types supported by AMQP.
const (
	ExchangeDirect  = "direct"
	ExchangeFanout  = "fanout"
	ExchangeTopic   = "topic"
	ExchangeHeaders = "headers"
)

// ExchangeConfig describes an exchange to declare.
type ExchangeConfig struct {
	Name       string
	Type       string // direct | fanout | topic | headers (defaults to direct)
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}

type Exchange struct {
	config      ExchangeConfig
	channelPool channel.ChannelPool
	mu          sync.Mutex
}

func NewExchange(config ExchangeConfig, channelPool channel.ChannelPool) *Exchange {
	return &Exchange{
		config:      config,
		channelPool: channelPool,
	}
}

func (e *Exchange) Declare(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	channel, err := e.channelPool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire channle: %w", err)
	}
	defer e.channelPool.Release(channel)

	err = channel.ExchangeDeclare(
		e.config.Name,
		e.config.Type,
		e.config.Durable,
		e.config.AutoDelete,
		e.config.Internal,
		e.config.NoWait,
		amqp.Table(e.config.Arguments),
	)
	if err != nil {
		return fmt.Errorf("exchange declare: %w", err)
	}

	return nil
}
