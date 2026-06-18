package topology

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueConfig describes a queue to declare.
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  map[string]interface{}
	// DLQ        *DLQConfig
}

type Queue struct {
	config      QueueConfig
	channelPool ChannelProvider
	mu          sync.Mutex
}

func NewQueue(config QueueConfig, channelPool ChannelProvider) *Queue {
	return &Queue{
		config:      config,
		channelPool: channelPool,
	}
}

func (q *Queue) Declare(ctx context.Context) (amqp.Queue, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	channel, err := q.channelPool.Acquire(ctx)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("acquire channel: %w", err)
	}

	queue, err := channel.QueueDeclare(
		q.config.Name,
		q.config.Durable,
		q.config.AutoDelete,
		q.config.Exclusive,
		q.config.NoWait,
		amqp.Table(q.config.Arguments),
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("queue declare: %w", err)
	}

	return queue, nil
}
