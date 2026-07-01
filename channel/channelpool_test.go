package channel

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq/connection"
)

// fakeConn là Connection giả không cần broker. Get luôn trả lỗi nên
// createChannel sẽ fail — phù hợp để test các nhánh closed/ctx/Max mà
// không cần một *amqp.Channel thật.
type fakeConn struct {
	getErr    error
	mu        sync.Mutex
	observers []connection.Observer
}

func (f *fakeConn) Connect() error { return nil }
func (f *fakeConn) Close() error   { return nil }

func (f *fakeConn) Get() (*amqp.Connection, error) {
	return nil, f.getErr
}

func (f *fakeConn) Register(o connection.Observer) {
	f.mu.Lock()
	f.observers = append(f.observers, o)
	f.mu.Unlock()
}

func newTestPool(t *testing.T, cfg PoolConfig) ChannelPool {
	t.Helper()
	p, err := New(cfg, &fakeConn{getErr: errors.New("no broker")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestAcquireOnClosedPool(t *testing.T) {
	p := newTestPool(t, PoolConfig{Min: 0, Max: 4})
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err := p.Acquire(context.Background())
	if !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("want ErrPoolClosed, got %v", err)
	}
}

func TestAcquireRespectsContext(t *testing.T) {
	// Max=0 → không bao giờ tạo được channel; Acquire phải tôn trọng ctx.
	p := newTestPool(t, PoolConfig{Min: 0, Max: 0})
	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := p.Acquire(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
}

func TestCloseIdempotentAndReleaseNil(t *testing.T) {
	p := newTestPool(t, PoolConfig{Min: 0, Max: 2})

	if err := p.Release(nil); err != nil {
		t.Fatalf("Release(nil): %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestConcurrentAcquireClose(t *testing.T) {
	p := newTestPool(t, PoolConfig{Min: 0, Max: 0})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = p.Acquire(context.Background())
		}()
	}

	time.Sleep(10 * time.Millisecond)
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait() // tất cả waiter phải được đánh thức qua done
}

func TestNewWarmError(t *testing.T) {
	_, err := New(
		PoolConfig{Min: 1, Max: 4},
		&fakeConn{getErr: errors.New("no broker")},
	)
	if err == nil {
		t.Fatal("want error when warm fails, got nil")
	}
}
