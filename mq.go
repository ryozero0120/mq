package mq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/connection"
	"github.com/ryozero0120/mq/observability"
	"github.com/ryozero0120/mq/reliability"
	"github.com/ryozero0120/mq/topology"
	"github.com/ryozero0120/mq/worker"
)

type mq struct {
	config          MqConfig
	logger          observability.Logger
	connManager     connection.Connection
	channelPool     channel.ChannelPool
	topologyManager topology.TopologyManager
}

type MQ interface {
	Subscriber(ctx context.Context, config SubscriberConfig, handler MessageHandler) Subscriber
	Publisher(config PublisherConfig) Publisher
	DeclareExchange(ctx context.Context, config topology.ExchangeConfig) error
	DeclareQueue(ctx context.Context, config topology.QueueConfig) (amqp.Queue, error)
	CreateBinding(ctx context.Context, config topology.BindingConfig) error
	Close() error
}

func NewMQ(ctx context.Context, config MqConfig, logger observability.Logger) (MQ, error) {
	connManager := connection.NewConnection(ctx, config.Connection, logger)

	if err := connManager.Connect(); err != nil {
		return nil, err
	}

	channelPool, err := channel.NewChannelPool(config.Pool, connManager)
	if err != nil {
		return nil, err
	}

	topologyManager := topology.NewTopologyManager(nil, channelPool)

	return &mq{
		config:          config,
		logger:          logger,
		connManager:     connManager,
		channelPool:     channelPool,
		topologyManager: topologyManager,
	}, nil
}

func (m *mq) Subscriber(ctx context.Context, config SubscriberConfig, handler MessageHandler) Subscriber {
	workerPool := worker.NewWorkerPool(
		worker.Config{
			Workers:         config.Concurrency,
			QueueSize:       config.Concurrency * 8,
			ShutdownTimeout: 10 * time.Second,
			JobTimout:       5 * time.Second,
			OnJobError: func(err error) {
				fmt.Print(err)
			},
		},
	)

	var ackManager reliability.Acker
	if !config.AutoAck {
		ackManager = reliability.NewAckManager(reliability.AckConfig{
			MultipleAck:   false,
			RequeueOnNack: config.RequeueOnNack,
		})
	}

	retryPolicy := reliability.NewExponentialRetryPolicyWithConfig(
		reliability.ExponentialRetryConfig{
			MaxRetries: 3,
		})

	dlqHandler := reliability.NewDLQHandler()

	subscriber := NewSubscriber(m.connManager, config, m.logger, ackManager, retryPolicy, dlqHandler, workerPool)
	subscriber.Subscribe(handler)

	return subscriber
}

func (m *mq) Publisher(config PublisherConfig) Publisher {
	return NewPublisher(config, m.channelPool)
}

func (m *mq) DeclareExchange(ctx context.Context, config topology.ExchangeConfig) error {
	return m.topologyManager.DeclareExchange(ctx, config)
}

func (m *mq) DeclareQueue(ctx context.Context, config topology.QueueConfig) (amqp.Queue, error) {
	return m.topologyManager.DeclareQueue(ctx, config)
}

func (m *mq) CreateBinding(ctx context.Context, config topology.BindingConfig) error {
	return m.topologyManager.CreateBinding(ctx, config)
}

func (m *mq) Close() error {
	var mErr []error

	if err := m.channelPool.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("channel pool close error: %w", err))
	}

	if err := m.connManager.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("connManager close error: %w", err))
	}

	if len(mErr) > 0 {
		return fmt.Errorf("errors closing mq: %v", mErr)
	}

	return nil
}
