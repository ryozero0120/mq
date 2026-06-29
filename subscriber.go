package mq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/observability"
	"github.com/ryozero0120/mq/reliability"
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
	conn        Connection
	config      SubscriberConfig
	logger      observability.Logger
	handler     MessageHandler
	workerPool  worker.Pool
	ackManager  reliability.Acker
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

type SubscriberFunc func(ctx context.Context, msg *Message) error

type SubscriberMiddleware interface {
	Intercept(ctx context.Context, msg *Message, next PublisherFunc) error
}

type MessageHandler interface {
	Handle(ctx context.Context, m *Message) error
}

type SubscribeHandler func(ctx context.Context, m *Message) error

func NewSubscriber(conn Connection, config SubscriberConfig, logger observability.Logger, workerPool worker.Pool, ackManager reliability.Acker) Subscriber {
	return &subscriber{
		conn:       conn,
		config:     config,
		logger:     logger,
		workerPool: workerPool,
		ackManager: ackManager,
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

func (s *subscriber) applyMiddleware(ctx context.Context, msg *Message, index int, next SubscriberFunc) error {
	if index >= len(s.middlewares) {
		return next(ctx, msg)
	}

	middleware := s.middlewares[index]
	return middleware.Intercept(ctx, msg, func(ctx context.Context, msg *Message) error {
		return s.applyMiddleware(ctx, msg, index+1, next)
	})
}

func (s *subscriber) onconnected() {
	s.logger.Info("subscriber on connected")
	s.Start(context.Background())
}

func (s *subscriber) ondisconnected() {
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

func (s *subscriber) doMsg(ctx context.Context, msg *Message) error {
	err := s.handler.Handle(ctx, msg)
	if err != nil {
		s.logger.Error("failed to do message", "error", err.Error())
	}
	return err
}

func (s *subscriber) failed(_ context.Context, d amqp.Delivery, _ *Message, _ error) error {
	if !s.config.AutoAck && s.ackManager != nil {
		s.ackManager.Nack(d, false)
	}
	return nil
}

func (s *subscriber) deliveryToMessage(d amqp.Delivery) *Message {
	headers := make(map[string]interface{})
	for k, v := range d.Headers {
		headers[k] = v
	}

	return &Message{
		ID:              d.MessageId,
		Headers:         headers,
		Body:            d.Body,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Timestamp:       d.Timestamp,
		Priority:        d.Priority,
		DeliveryMode:    DeliveryMode(d.DeliveryMode),
	}
}
