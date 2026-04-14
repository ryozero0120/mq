package mq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SubscriberConfig struct {
	Queue         string
	Tag           string
	AutoAck       bool
	PrefetchCount int
	PrefetchSize  int
	Concurrency   int
}

type subscriber struct {
	cfg     SubscriberConfig
	conn    Connection
	handler Handler

	ch         *amqp.Channel
	deliveries <-chan amqp.Delivery

	wg     sync.WaitGroup
	stopCh chan struct{}
}

type Subscriber interface {
	Subscribe(ctx context.Context, fn Handler) error
	Start(ctx context.Context) error
	Stop() error
}

type Handler interface {
	Handle(ctx context.Context, m *Message) error
}

type SubscribeHandler func(ctx context.Context, m *Message) error

func NewSubscriber(cfg SubscriberConfig, conn Connection) Subscriber {
	return &subscriber{
		cfg:  cfg,
		conn: conn,
	}
}

func (s *subscriber) Subscribe(ctx context.Context, h Handler) error {
	if h == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	s.handler = h

	return nil
}

func (s *subscriber) Start(ctx context.Context) error {
	connection, err := s.conn.Get()
	if err != nil {
		return err
	}

	s.conn.Register(s)

	s.ch, err = connection.Channel()
	if err != nil {
		return err
	}

	if err := s.ch.Qos(s.cfg.PrefetchCount, s.cfg.PrefetchSize, false); err != nil {
		s.ch.Close()
		return err
	}

	s.deliveries, err = s.ch.Consume(s.cfg.Queue, s.cfg.Tag, s.cfg.AutoAck, false, false, false, nil)
	if err != nil {
		s.ch.Close()
		return err
	}

	s.wg.Add(1)

	go s.doDelivery(ctx)

	return nil
}

func (s *subscriber) Stop() error {
	slog.Error("subscriber stop")

	close(s.stopCh)

	s.wg.Wait()

	if s.ch != nil {
		if err := s.ch.Close(); err != nil {
			slog.Error("failed to close channel", "error", err.Error())
		}
	}

	return nil
}

func (s *subscriber) onconnected() {
	s.Start(context.Background())
}

func (s *subscriber) ondisconnected() {
	// s.Start(context.Background())
}

func (s *subscriber) doDelivery(ctx context.Context) {
	defer s.wg.Done()

	for {
		select {
		case <-ctx.Done():
			slog.Info("subscriber ctx done")
			return
		case <-s.stopCh:
			slog.Info("subscriber stop chan")
			return
		case d, ok := <-s.deliveries:
			if !ok {
				slog.Error("delivery channel closed")
				return
			}

			dm := s.deliveryToMessage(d)

			if err := s.doMsg(ctx, dm); err != nil {
				slog.Error("failed to do message", "error", err.Error())
			}

			if !s.cfg.AutoAck {
				if err := d.Ack(false); err != nil {
					slog.Error("failed to ack message", "error", err.Error())
				}
			}
		}
	}
}

func (s *subscriber) doMsg(ctx context.Context, m *Message) error {
	return s.handler.Handle(ctx, m)
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
