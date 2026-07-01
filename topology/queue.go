package topology

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/reliability"
)

// QueueConfig describes a queue to declare.
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  map[string]interface{}
	DLQ        *reliability.DLQConfig
}

type Queue struct {
	config      QueueConfig
	channelPool channel.ChannelPool
	declared    atomic.Bool
	mu          sync.Mutex
}

func NewQueue(config QueueConfig, channelPool channel.ChannelPool) *Queue {
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
	defer q.channelPool.Release(channel)

	queue, err := channel.QueueDeclare(
		q.config.Name,
		q.config.Durable,
		q.config.AutoDelete,
		q.config.Exclusive,
		q.config.NoWait,
		q.buildArgs(),
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("queue declare: %w", err)
	}

	return queue, nil
}

func (e *Queue) IsDeclared() bool {
	return e.declared.Load()
}

// buildArgs merges user arguments with dead-letter arguments derived from the
// DLQ config.
func (q *Queue) buildArgs() amqp.Table {
	args := amqp.Table{}
	for k, v := range q.config.Arguments {
		args[k] = v
	}

	if q.config.DLQ != nil {
		if q.config.DLQ.Exchange != "" {
			args["x-dead-letter-exchange"] = q.config.DLQ.Exchange
		}
		rk := q.config.DLQ.RoutingKey
		if rk == "" {
			rk = q.config.Name
		}
		args["x-dead-letter-routing-key"] = rk
	}

	return args
}
