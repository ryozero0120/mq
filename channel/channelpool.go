package channel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/connection"
)

var (
	ErrPoolClosed     = errors.New("channel pool is closed")
	ErrAcquireTimeout = errors.New("channel pool acquire timeout")
)

type PoolConfig struct {
	Min int
	Max int

	// IdleTimeout: thời gian tối đa một channel được nằm rỗi trong pool trước
	// khi bị đóng. 0 = không giới hạn.
	IdleTimeout time.Duration
	// MaxLifetime: tuổi thọ tối đa của một channel kể từ lúc tạo. 0 = không giới hạn.
	MaxLifetime time.Duration
	// AcquireTimeout: thời gian tối đa chờ lấy một channel khi pool đã đầy. 0 = chờ vô hạn.
	AcquireTimeout time.Duration
}

// pooledChannel bọc *amqp.Channel kèm metadata để áp dụng IdleTimeout/MaxLifetime.
type pooledChannel struct {
	ch         *amqp.Channel
	createdAt  time.Time
	lastUsedAt time.Time
}

type channelPool struct {
	config     PoolConfig
	connection connection.Connection
	channels   chan *pooledChannel
	done       chan struct{}
	mu         sync.Mutex
	closed     bool
	total      int                              // channel đang mở: trong pool + đang mượn
	inUse      map[*amqp.Channel]*pooledChannel // channel đang được mượn (giữ metadata cho Release)
}

type ChannelPool interface {
	Acquire(ctx context.Context) (*amqp.Channel, error)
	Release(ch *amqp.Channel) error
	Close() error
}

func New(config PoolConfig, conn connection.Connection) (ChannelPool, error) {
	pool := &channelPool{
		config:     config,
		connection: conn,
		channels:   make(chan *pooledChannel, config.Max),
		done:       make(chan struct{}),
		inUse:      make(map[*amqp.Channel]*pooledChannel),
	}

	conn.Register(pool) // nhận OnConnected/OnDisconnected

	if err := pool.warm(); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to pre-populate channel pool: %w", err)
	}

	if iv := reapInterval(config); iv > 0 {
		go pool.reaper(iv)
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
		if p.closed || p.total >= p.config.Min {
			p.mu.Unlock()
			return nil
		}
		p.total++
		p.mu.Unlock()

		pc, err := p.createChannel()
		if err != nil {
			p.decr()
			return err
		}

		select {
		case p.channels <- pc:
		default:
			pc.ch.Close()
			p.decr()
		}
	}
}

func (p *channelPool) Acquire(ctx context.Context) (*amqp.Channel, error) {
	var timeout <-chan time.Time
	if p.config.AcquireTimeout > 0 {
		t := time.NewTimer(p.config.AcquireTimeout)
		defer t.Stop()
		timeout = t.C
	}

	for {
		// fast-path: lấy channel còn dùng được có sẵn trong pool
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case <-timeout:
			return nil, ErrAcquireTimeout
		case pc := <-p.channels:
			if !p.stale(pc) {
				return p.checkout(pc), nil
			}
			p.discard(pc) // chết / hết đời / rỗi quá lâu → bỏ
			continue
		default:
		}

		// pool rỗng: tạo mới nếu chưa chạm Max
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		if p.total < p.config.Max {
			p.total++
			p.mu.Unlock()
			pc, err := p.createChannel()
			if err != nil {
				p.decr()
				return nil, err
			}
			return p.checkout(pc), nil
		}
		p.mu.Unlock()

		// đã chạm Max: chờ channel được trả về / ctx hủy / pool đóng / acquire timeout
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case <-timeout:
			return nil, ErrAcquireTimeout
		case pc := <-p.channels:
			if !p.stale(pc) {
				return p.checkout(pc), nil
			}
			p.discard(pc)
		}
	}
}

func (p *channelPool) Release(ch *amqp.Channel) error {
	if ch == nil {
		return nil
	}

	p.mu.Lock()
	pc, ok := p.inUse[ch]
	if ok {
		delete(p.inUse, ch)
	}
	closed := p.closed
	p.mu.Unlock()

	if pc == nil {
		// không phải channel do pool cấp (hoặc đã release) → đóng cho an toàn
		return ch.Close()
	}

	// pool đóng / channel chết / hết tuổi thọ → không tái sử dụng
	if closed || ch.IsClosed() || p.overLifetime(pc) {
		p.decr()
		return ch.Close()
	}

	pc.lastUsedAt = time.Now()
	select {
	case p.channels <- pc:
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
		case pc := <-p.channels:
			if err := pc.ch.Close(); err != nil {
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
		case pc := <-p.channels:
			pc.ch.Close()
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

// checkout ghi nhận channel đang được mượn rồi trả *amqp.Channel cho caller.
func (p *channelPool) checkout(pc *pooledChannel) *amqp.Channel {
	p.mu.Lock()
	p.inUse[pc.ch] = pc
	p.mu.Unlock()
	return pc.ch
}

// discard đóng channel và giảm total.
func (p *channelPool) discard(pc *pooledChannel) {
	if pc != nil && pc.ch != nil {
		pc.ch.Close()
	}
	p.decr()
}

// stale trả true nếu channel không còn dùng được: đã đóng, hết tuổi thọ,
// hoặc nằm rỗi quá IdleTimeout.
func (p *channelPool) stale(pc *pooledChannel) bool {
	if pc == nil || pc.ch == nil || pc.ch.IsClosed() {
		return true
	}
	if p.overLifetime(pc) {
		return true
	}
	if p.config.IdleTimeout > 0 && time.Since(pc.lastUsedAt) >= p.config.IdleTimeout {
		return true
	}
	return false
}

func (p *channelPool) overLifetime(pc *pooledChannel) bool {
	return p.config.MaxLifetime > 0 && time.Since(pc.createdAt) >= p.config.MaxLifetime
}

// reaper định kỳ quét pool, đóng các channel hết đời / rỗi quá lâu ngay cả khi
// pool đang nhàn rỗi (không có Acquire).
func (p *channelPool) reaper(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.reap()
		}
	}
}

func (p *channelPool) reap() {
	n := len(p.channels)
	for i := 0; i < n; i++ {
		select {
		case pc := <-p.channels:
			if p.stale(pc) {
				pc.ch.Close()
				p.decr()
				continue
			}
			select {
			case p.channels <- pc:
			default:
				pc.ch.Close()
				p.decr()
			}
		default:
			return
		}
	}
}

func (p *channelPool) createChannel() (*pooledChannel, error) {
	conn, err := p.connection.Get()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	now := time.Now()
	return &pooledChannel{ch: ch, createdAt: now, lastUsedAt: now}, nil
}

// reapInterval chọn chu kỳ quét dựa trên timeout nhỏ nhất được bật (tối thiểu 1s).
// Trả 0 khi không có IdleTimeout lẫn MaxLifetime → không chạy reaper.
func reapInterval(config PoolConfig) time.Duration {
	d := config.IdleTimeout
	if config.MaxLifetime > 0 && (d == 0 || config.MaxLifetime < d) {
		d = config.MaxLifetime
	}
	if d <= 0 {
		return 0
	}
	d /= 2
	if d < time.Second {
		d = time.Second
	}
	return d
}
