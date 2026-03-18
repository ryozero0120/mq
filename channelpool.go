package mq

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PoolConfig struct {
	Min int
	Max int
}

type channelPool struct {
	cfg        PoolConfig
	connection Connection
	channels   chan *amqp.Channel
	mu         sync.Mutex
	closed     bool
}

type ChannelPool interface {
	Acquire(ctx context.Context) (*amqp.Channel, error)
	Release(ch *amqp.Channel) error
	Close() error
}

func NewChannelPool(cfg PoolConfig, conn Connection) ChannelPool {
	pool := &channelPool{
		cfg:        cfg,
		connection: conn,
		channels:   make(chan *amqp.Channel, 1),
		closed:     false,
	}

	for i := 0; i < cfg.Max; i++ {
		ch, err := pool.createChannel()
		if err != nil {
			pool.Close()
		}

		pool.channels <- ch
	}

	return pool
}

func (p *channelPool) Acquire(ctx context.Context) (*amqp.Channel, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("channel pool is closed")
	}
	p.mu.Unlock()

	// for {
	// 	select {
	// 	case ch := <-p.channels:
	// 		if !ch.IsClosed() {
	// 			return ch, nil
	// 		}
	// 		continue
	// 	default:
	// 		//
	// 	}

	// 	break
	// }

	return <-p.channels, nil
}

func (p *channelPool) Release(ch *amqp.Channel) error {
	p.mu.Lock()
	if p.closed {
		return ch.Close()
	}
	p.mu.Unlock()

	select {
	case p.channels <- ch:
		return nil
	default:
		return ch.Close()
	}
}

func (p *channelPool) Close() error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()

	var pErr error

	for {
		select {
		case ch := <-p.channels:
			if err := ch.Close(); err != nil {
				pErr = err
			}
		default:
			return pErr
		}

	}
}

func (p *channelPool) createChannel() (*amqp.Channel, error) {
	conn, err := p.connection.Get()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return ch, nil
}
