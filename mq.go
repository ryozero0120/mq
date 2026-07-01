package mq

import (
	"context"
	"fmt"
	"runtime"
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
	config      MqConfig
	logger      observability.Logger
	conn        connection.Connection
	channelPool channel.ChannelPool
	topology    topology.TopologyInstaller
}

type MQ interface {
	Subscriber(ctx context.Context, config SubscriberConfig, retryPolicy reliability.RetryPolicy, dlqHandler reliability.DLQExecutor, workerPool worker.Pool, handler MessageHandler) Subscriber
	Publisher(config PublisherConfig) Publisher
	DeclareExchange(ctx context.Context, config topology.ExchangeConfig) error
	DeclareQueue(ctx context.Context, config topology.QueueConfig) (amqp.Queue, error)
	CreateBinding(ctx context.Context, config topology.BindingConfig) error
	Close() error
}

func NewMQ(ctx context.Context, config MqConfig, logger observability.Logger) (MQ, error) {
	conn := connection.New(ctx, config.Connection, logger)

	if err := conn.Connect(); err != nil {
		return nil, err
	}

	channelPool, err := channel.New(config.Pool, conn)
	if err != nil {
		return nil, err
	}

	topology := topology.New(logger, channelPool)

	return &mq{
		config:      config,
		logger:      logger,
		conn:        conn,
		channelPool: channelPool,
		topology:    topology,
	}, nil
}

func (m *mq) Subscriber(ctx context.Context, config SubscriberConfig, retryPolicy reliability.RetryPolicy, dlqHandler reliability.DLQExecutor, workerPool worker.Pool, handler MessageHandler) Subscriber {
	if workerPool == nil {
		workerPool = worker.NewWorkerPool(
			worker.Config{
				Workers:         runtime.NumCPU(),
				QueueSize:       runtime.NumCPU() * 8,
				ShutdownTimeout: 10 * time.Second,
				JobTimeout:      5 * time.Second,
				OnJobError: func(err error) {
					fmt.Print(err)
				},
			},
		)
	}

	var ackManager reliability.Acker
	if !config.AutoAck {
		ackManager = reliability.NewAckManager(reliability.AckConfig{
			MultipleAck:   false,
			RequeueOnNack: config.RequeueOnNack,
		})
	}

	if retryPolicy == nil {
		retryPolicy = reliability.NewFixedRetryPolicyWithConfig(
			reliability.FixedRetryConfig{
				MaxRetries: 3,
				Delay:      1000 * time.Millisecond,
			})
	}

	subscriber := NewSubscriber(m.conn, m.channelPool, config, m.logger, ackManager, m.topology, retryPolicy, dlqHandler, workerPool)
	subscriber.Subscribe(handler)

	return subscriber
}

func (m *mq) Publisher(config PublisherConfig) Publisher {
	return NewPublisher(config, m.channelPool)
}

func (m *mq) DeclareExchange(ctx context.Context, config topology.ExchangeConfig) error {
	return m.topology.DeclareExchange(ctx, config)
}

func (m *mq) DeclareQueue(ctx context.Context, config topology.QueueConfig) (amqp.Queue, error) {
	return m.topology.DeclareQueue(ctx, config)
}

func (m *mq) CreateBinding(ctx context.Context, config topology.BindingConfig) error {
	return m.topology.CreateBinding(ctx, config)
}

func (m *mq) Close() error {
	var mErr []error

	if err := m.channelPool.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("channel pool close error: %w", err))
	}

	if err := m.conn.Close(); err != nil {
		mErr = append(mErr, fmt.Errorf("connection close error: %w", err))
	}

	if len(mErr) > 0 {
		return fmt.Errorf("errors closing mq: %v", mErr)
	}

	return nil
}
