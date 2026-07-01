package mq

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/connection"
	"github.com/ryozero0120/mq/delivery"
	"github.com/ryozero0120/mq/observability"
	"github.com/ryozero0120/mq/reliability"
	"github.com/ryozero0120/mq/topology"
	"github.com/ryozero0120/mq/worker"
)

type SubscriberConfig struct {
	Queue         string
	Tag           string
	AutoAck       bool
	PrefetchCount int
	PrefetchSize  int
	Concurrency   int
	RequeueOnNack bool
}

type subscriber struct {
	conn        connection.Connection
	channelPool channel.ChannelPool
	config      SubscriberConfig
	logger      observability.Logger
	ackManager  reliability.Acker
	topology    topology.TopologyInstaller
	retryPolicy reliability.RetryPolicy
	dlqHandler  reliability.DLQExecutor
	workerPool  worker.Pool
	handler     MessageHandler
	middlewares []SubscriberMiddleware
	ch          *amqp.Channel
	deliveries  <-chan amqp.Delivery
	done        chan struct{}
	wg          sync.WaitGroup
	mu          sync.RWMutex
}

type Subscriber interface {
	Subscribe(handler MessageHandler) error
	Start(ctx context.Context) error
	Stop() error
	Use(middleware SubscriberMiddleware)
}

type SubscriberFunc func(ctx context.Context, msg *delivery.DM) error

type SubscriberMiddleware interface {
	Intercept(ctx context.Context, msg *delivery.DM, next PublisherFunc) error
}

type MessageHandler interface {
	Handle(ctx context.Context, m *delivery.DM) error
}

type SubscribeHandler func(ctx context.Context, m *delivery.DM) error

func NewSubscriber(conn connection.Connection, channelPool channel.ChannelPool, config SubscriberConfig, logger observability.Logger, ackManager reliability.Acker, topology topology.TopologyInstaller, retryPolicy reliability.RetryPolicy, dlqHandler reliability.DLQExecutor, workerPool worker.Pool) Subscriber {
	return &subscriber{
		conn:        conn,
		channelPool: channelPool,
		config:      config,
		logger:      logger,
		ackManager:  ackManager,
		topology:    topology,
		retryPolicy: retryPolicy,
		dlqHandler:  dlqHandler,
		workerPool:  workerPool,
		done:        make(chan struct{}),
	}
}

func (s *subscriber) Subscribe(handler MessageHandler) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	s.handler = handler

	return nil
}

func (s *subscriber) Start(ctx context.Context) error {
	conn, err := s.conn.Get()
	if err != nil {
		return err
	}

	s.conn.Register(s)

	s.ch, err = conn.Channel()
	if err != nil {
		return err
	}

	if err := s.ch.Qos(s.config.PrefetchCount, s.config.PrefetchSize, false); err != nil {
		s.ch.Close()
		return err
	}

	s.deliveries, err = s.ch.Consume(s.config.Queue, s.config.Tag, s.config.AutoAck, false, false, false, nil)
	if err != nil {
		s.ch.Close()
		return err
	}

	if s.workerPool != nil {
		go s.workerPool.Start()
	}

	s.wg.Add(1)
	go s.doDeliveries(ctx)

	return nil
}

func (s *subscriber) Stop() error {
	close(s.done)

	s.wg.Wait()

	if s.workerPool != nil {
		s.workerPool.Stop()
	}

	if s.ch != nil {
		if err := s.ch.Close(); err != nil {
			s.logger.Error("failed to close channel", "error", err.Error())
		}
	}

	return nil
}

func (s *subscriber) Use(mw SubscriberMiddleware) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.middlewares = append(s.middlewares, mw)
}

func (s *subscriber) applyMiddleware(ctx context.Context, msg *delivery.DM, index int, next SubscriberFunc) error {
	if index >= len(s.middlewares) {
		return next(ctx, msg)
	}

	middleware := s.middlewares[index]
	return middleware.Intercept(ctx, msg, func(ctx context.Context, msg *delivery.DM) error {
		return s.applyMiddleware(ctx, msg, index+1, next)
	})
}

func (s *subscriber) OnConnected() {
	s.logger.Info("subscriber on connected")
	s.Start(context.Background())
}

func (s *subscriber) OnDisconnected() {
	// s.Start(context.Background())
}

func (s *subscriber) doDeliveries(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("subscriber ctx done")
			return
		case <-s.done:
			s.logger.Info("subscriber stop chan")
			return
		case d, ok := <-s.deliveries:
			if !ok {
				s.logger.Error("delivery channel closed")
				return
			}

			if s.workerPool != nil {
				job := worker.NewTask(func(ctx context.Context) error {
					return s.doDelivery(ctx, d)
				})
				if err := s.workerPool.Submit(job); err != nil {
					if !s.config.AutoAck && s.ackManager != nil {
						s.ackManager.Nack(d, s.config.RequeueOnNack)
					}
				}
			} else {
				s.doDelivery(ctx, d)
			}
		}
	}
}

func (s *subscriber) doDelivery(ctx context.Context, d amqp.Delivery) error {
	msg := s.deliveryToMessage(d)

	var err error
	if len(s.middlewares) > 0 {
		err = s.applyMiddleware(ctx, msg, 0, s.doMsg)
	} else {
		err = s.doMsg(ctx, msg)
	}

	if err != nil {
		return s.failed(ctx, d, msg, err)
	}

	if !s.config.AutoAck && s.ackManager != nil {
		if err := s.ackManager.Ack(d); err != nil {
			s.logger.Error("failed to ack message", "error", err.Error())
		}
	}

	return nil
}

func (s *subscriber) doMsg(ctx context.Context, msg *delivery.DM) error {
	err := s.handler.Handle(ctx, msg)
	if err != nil {
		s.logger.Error("failed to do message", "error", err.Error())
	}
	return err
}

func (s *subscriber) failed(ctx context.Context, d amqp.Delivery, m *delivery.DM, cause error) error {
	attempt := m.GetRetryCount()

	if s.retryPolicy.ShouldRetry(cause, attempt) {
		delay := s.retryPolicy.NextDelay(attempt)

		if err := s.retry(ctx, d, delay, attempt+1); err != nil {
			s.logger.Error("failed to schedule retry", "error", err.Error())
			// fallback: requeue thô để không mất message (không tăng được count)
			if !s.config.AutoAck && s.ackManager != nil {
				s.ackManager.Nack(d, true)
			}
			return nil
		}

		s.logger.Warn("retrying message", "msg id", m.ID, "attempt", attempt, "delay", delay.String())

		// copy đã vào delay queue → ack bản gốc
		if !s.config.AutoAck && s.ackManager != nil {
			return s.ackManager.Ack(d)
		}
		return nil
	}

	// hết retry → DLQ
	if s.dlqHandler != nil {
		if err := s.dlqHandler.Handle(ctx, m); err != nil {
			s.logger.Error("failed to send to DLQ", "error", err.Error())
			// giữ lại để thử DLQ sau, tránh mất message
			if !s.config.AutoAck && s.ackManager != nil {
				s.ackManager.Nack(d, true)
			}
			return nil
		}
	}

	if !s.config.AutoAck && s.ackManager != nil {
		return s.ackManager.Ack(d)
	}

	return nil
}

// retry declare delay queue (qua topology, idempotent) rồi publish
// một bản copy có x-retry-count đã tăng vào đó. Message nằm chờ hết x-message-ttl
// rồi được broker dead-letter ngược về queue gốc.
func (s *subscriber) retry(ctx context.Context, d amqp.Delivery, delay time.Duration, nextCount int) error {
	ttl := int32(delay.Milliseconds())
	dq := fmt.Sprintf("%s.delay.%d", s.config.Queue, delay.Milliseconds())

	if _, err := s.topology.DeclareQueue(ctx, topology.QueueConfig{
		Name:    dq,
		Durable: true,
		Arguments: map[string]interface{}{
			"x-message-ttl":             ttl,
			"x-dead-letter-exchange":    "",             // default exchange
			"x-dead-letter-routing-key": s.config.Queue, // dead-letter ngược về Q
			"x-expires":                 ttl + 60000,    // tự xoá khi idle (ttl + 60s), phải > ttl
		},
	}); err != nil {
		return err
	}

	ch, err := s.channelPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer s.channelPool.Release(ch)

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-retry-count"] = int32(nextCount)

	return ch.PublishWithContext(ctx, "", dq, false, false, amqp.Publishing{
		Headers:         headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Body:            d.Body,
	})
}

// deliveryToMessage converts AMQP delivery to Message
func (s *subscriber) deliveryToMessage(d amqp.Delivery) *delivery.DM {
	headers := make(map[string]interface{})
	for k, v := range d.Headers {
		headers[k] = v
	}

	retryCount := 0
	if rc, ok := headers["x-retry-count"].(int32); ok {
		retryCount = int(rc)
	}

	return &delivery.DM{
		ID:              d.MessageId,
		Headers:         headers,
		Body:            d.Body,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Timestamp:       d.Timestamp,
		Priority:        d.Priority,
		DeliveryMode:    delivery.DeliveryMode(d.DeliveryMode),
		Context: delivery.Context{
			SpanID:     "",
			TraceID:    "",
			RetryCount: retryCount,
		},
	}
}
