package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/connection"
)

var ErrPoolClosed = errors.New("channel pool is closed")

type PoolConfig struct {
	Min int
	Max int
}

type channelPool struct {
	cfg        PoolConfig
	connection connection.Connection
	channels   chan *amqp.Channel
	done       chan struct{}
	mu         sync.Mutex
	closed     bool
	total      int // channel đang mở: trong pool + đang mượn
}

type ChannelPool interface {
	Acquire(ctx context.Context) (*amqp.Channel, error)
	Release(ch *amqp.Channel) error
	Close() error
}

func New(cfg PoolConfig, conn connection.Connection) (ChannelPool, error) {
	pool := &channelPool{
		cfg:        cfg,
		connection: conn,
		channels:   make(chan *amqp.Channel, cfg.Max),
		done:       make(chan struct{}),
	}

	conn.Register(pool) // nhận OnConnected/OnDisconnected

	if err := pool.warm(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to pre-populate channel pool: %w", err)
	}

	return pool, nil
}

func (p *channelPool) decr() {
	p.mu.Lock()
	p.total--
	p.mu.Unlock()
}

// warm tạo channel cho tới khi đạt Min, đếm vào total và push non-blocking.
func (p *channelPool) warm() error {
	for {
		p.mu.Lock()
		if p.closed || p.total >= p.cfg.Min {
			p.mu.Unlock()
			return nil
		}
		p.total++
		p.mu.Unlock()

		ch, err := p.createChannel()
		if err != nil {
			p.decr()
			return err
		}

		select {
		case p.channels <- ch:
		default:
			ch.Close()
			p.decr()
		}
	}
}

func (p *channelPool) Acquire(ctx context.Context) (*amqp.Channel, error) {
	for {
		// fast-path: lấy channel sống có sẵn trong pool
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case ch := <-p.channels:
			if ch != nil && !ch.IsClosed() {
				return ch, nil
			}
			p.decr() // channel chết, bỏ đi
			continue
		default:
		}

		// pool rỗng: tạo mới nếu chưa chạm Max
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if p.total < p.cfg.Max {
			p.total++
			p.mu.Unlock()
			ch, err := p.createChannel()
			if err != nil {
				p.decr()
				return nil, err
			}
			return ch, nil
		}
		p.mu.Unlock()

		// đã chạm Max: chờ channel được trả về / ctx hủy / pool đóng
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case ch := <-p.channels:
			if ch != nil && !ch.IsClosed() {
				return ch, nil
			}
			p.decr()
		}
	}
}

func (p *channelPool) Release(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}

	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed || ch.IsClosed() {
		p.decr()
		return ch.Close()
	}

	select {
	case p.channels <- ch:
		return nil
	default:
		p.decr()
		return ch.Close()
	}
}

func (p *channelPool) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	close(p.done)
	p.mu.Unlock()

	var pErr error
	for {
		select {
		case ch := <-p.channels:
			if err := ch.Close(); err != nil {
				pErr = err
			}
			p.decr()
		default:
			return pErr
		}
	}
}

// OnDisconnected flush các channel đã chết còn trong buffer khi mất kết nối.
func (p *channelPool) OnDisconnected() {
	for {
		select {
		case ch := <-p.channels:
			ch.Close()
			p.decr()
		default:
			return
		}
	}
}

// OnConnected nạp lại pool sau khi kết nối phục hồi (best-effort).
func (p *channelPool) OnConnected() {
	_ = p.warm() // nếu lỗi, Acquire sẽ lazy-create khi cần
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
