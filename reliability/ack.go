package reliability

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acker interface {
	Ack(delivery amqp.Delivery) error

	Nack(delivery amqp.Delivery, requeue bool) error

	Reject(delivery amqp.Delivery, requeue bool) error
}

type AckConfig struct {
	MultipleAck   bool
	RequeueOnNack bool
}

type AckManager struct {
	config AckConfig
}

func NewAckManager(config AckConfig) *AckManager {
	return &AckManager{
		config: config,
	}
}

func (m *AckManager) Ack(delivery amqp.Delivery) error {
	err := delivery.Ack(m.config.MultipleAck)
	if err != nil {
		return fmt.Errorf("failed to ACK message: %w", err)
	}

	return nil
}

func (m *AckManager) Nack(delivery amqp.Delivery, requeue bool) error {
	requeueFlag := requeue
	if !requeue {
		requeueFlag = m.config.RequeueOnNack
	}

	err := delivery.Nack(m.config.MultipleAck, requeueFlag)
	if err != nil {
		return fmt.Errorf("failed to NACK message: %w", err)
	}

	return nil
}

func (m *AckManager) Reject(delivery amqp.Delivery, requeue bool) error {
	err := delivery.Reject(requeue)
	if err != nil {
		return fmt.Errorf("failed to REJECT message: %w", err)
	}

	return nil
}
