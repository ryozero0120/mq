package mq

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ConnectionConfig struct {
	URL       string
	Heartbeat time.Duration
}

type Observer interface {
	onconnected()
	ondisconnected()
}

type connection struct {
	cfg       ConnectionConfig
	conn      *amqp.Connection
	observers []Observer
	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	closeChan chan *amqp.Error
}

type Connection interface {
	Connect() error
	Close() error
	Get() (*amqp.Connection, error)
	Register(o Observer)
}

func NewConnection(cfg ConnectionConfig) Connection {
	ctx, cancel := context.WithCancel(context.Background())
	return &connection{
		cfg:       cfg,
		ctx:       ctx,
		cancel:    cancel,
		observers: make([]Observer, 0),
		closeChan: make(chan *amqp.Error, 1),
	}
}

func (c *connection) Connect() error {
	if err := c.create(); err != nil {
		return err
	}

	go c.reconnect()

	return nil
}

func (c *connection) Close() error {
	c.cancel()

	if c.conn != nil && !c.conn.IsClosed() {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %w", err)
		}
	}

	return nil
}

func (c *connection) Get() (*amqp.Connection, error) {
	if c.conn.IsClosed() {
		return nil, fmt.Errorf("connection is closed")
	}

	return c.conn, nil
}

func (c *connection) Register(o Observer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.observers = append(c.observers, o)
}

func (c *connection) create() error {
	amqpCfg := amqp.Config{
		Heartbeat: c.cfg.Heartbeat,
	}

	conn, err := amqp.DialConfig(c.cfg.URL, amqpCfg)
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.conn = conn
	conn.NotifyClose(c.closeChan)
	c.mu.Unlock()

	return nil
}

func (c *connection) retry() bool {
	operation := func() error {
		return c.create()
	}

	b := backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(1*time.Second),
		backoff.WithMaxElapsedTime(0), // never stop
		backoff.WithMaxInterval(5*time.Second),
		backoff.WithMultiplier(1.5),
		backoff.WithRandomizationFactor(0.1),
	)

	if err := backoff.Retry(operation, b); err != nil {
		return false
	}

	return true
}

func (c *connection) reconnect() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case amqpErr, ok := <-c.closeChan:
			if !ok {
				return
			}

			var connErr error
			if amqpErr != nil {
				connErr = amqpErr
			} else {
				connErr = fmt.Errorf("connection closed unexpectedly")
			}

			log.Printf("[ConnectionManager] connection lost: %v", connErr)
			c.onDisconnected()
			if c.retry() {
				c.onConnected()
				log.Printf("[ConnectionManager] connection restored")
			} else {
				log.Printf("[ConnectionManager] reconnection failed, giving up")
				return
			}
		}
	}
}

func (c *connection) onDisconnected() {
	for _, observer := range c.observers {
		go observer.ondisconnected()
	}
}

func (c *connection) onConnected() {
	for _, observer := range c.observers {
		go observer.onconnected()
	}
}
