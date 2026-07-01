// Example: kiểm chứng retry theo Pattern 2 (delay-tier queues) + DLQ.
//
// Một handler LUÔN fail. Subscriber sẽ:
//   - Mỗi lần fail: publish bản copy (x-retry-count+1) vào queue delay
//     "orders.q.delay.{ttl}" (x-message-ttl = NextDelay, DLX về queue gốc).
//     => FixedRetryPolicy: delay cố định 1s mỗi lần retry (broker giữ chỗ,
//     không block worker; mọi lần retry dùng chung 1 delay queue).
//   - Sau MaxRetries: đẩy sang DLQ (exchange "dlq" / queue "dlq.v1").
//
// Một watcher consume "dlq.v1" để in message dead-letter.
//
// Yêu cầu: RabbitMQ ở amqp://guest:guest@localhost:5672/
//
//	docker run --rm -p 5672:5672 rabbitmq:3
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ryozero0120/mq"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/connection"
	"github.com/ryozero0120/mq/delivery"
	"github.com/ryozero0120/mq/reliability"
	"github.com/ryozero0120/mq/topology"
)

const amqpURL = "amqp://guest:guest@localhost:5672/"

// ---- logger đơn giản ra stdout (implements observability.Logger) ----

type consoleLogger struct{}

func (consoleLogger) Debug(msg string, f ...interface{}) { logline("DEBUG", msg, f) }
func (consoleLogger) Info(msg string, f ...interface{})  { logline("INFO", msg, f) }
func (consoleLogger) Warn(msg string, f ...interface{})  { logline("WARN", msg, f) }
func (consoleLogger) Error(msg string, f ...interface{}) { logline("ERROR", msg, f) }

func logline(level, msg string, f []interface{}) {
	if len(f) > 0 {
		log.Printf("[%-5s] %s %v", level, msg, f)
	} else {
		log.Printf("[%-5s] %s", level, msg)
	}
}

// ---- handler LUÔN fail để kích hoạt retry rồi DLQ ----

type failingHandler struct{}

func (failingHandler) Handle(_ context.Context, m *delivery.DM) error {
	log.Printf("  >> [orders.q] xử lý msg=%s attempt=%d -> FAIL", m.ID, m.GetRetryCount())
	return fmt.Errorf("always fail")
}

// ---- watcher DLQ ----

type dlqWatcher struct{}

func (dlqWatcher) Handle(_ context.Context, m *delivery.DM) error {
	log.Printf("  ** [DLQ dlq.v1] NHẬN dead-letter: id=%s body=%q retryCount=%d", m.ID, string(m.Body), m.GetRetryCount())
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := mq.NewMQ(ctx, mq.MqConfig{
		Connection: connection.ConnectionConfig{URL: amqpURL, Heartbeat: 10 * time.Second},
		Pool:       channel.PoolConfig{Min: 2, Max: 10},
	}, consoleLogger{})
	if err != nil {
		log.Fatalf("NewMQ: %v", err)
	}
	defer client.Close()

	// topology chính + DLQ (khớp DLQConfig mặc định: exchange "dlq", queue "dlq.v1", rk "dlq")
	must(client.DeclareExchange(ctx, topology.ExchangeConfig{Name: "demo", Type: "direct", Durable: true}), "declare demo")
	must(client.DeclareExchange(ctx, topology.ExchangeConfig{Name: "dlq", Type: "direct", Durable: true}), "declare dlq")
	declareAndBind(ctx, client, "orders.q", "demo", "orders")
	declareAndBind(ctx, client, "dlq.v1", "dlq", "dlq")

	// subscriber chính: luôn fail -> retry (fixed 1s) -> DLQ
	sub := client.Subscriber(ctx,
		mq.SubscriberConfig{
			Queue:         "orders.q",
			Tag:           "orders-consumer",
			AutoAck:       false,
			PrefetchCount: 1,
			RequeueOnNack: false,
		},
		reliability.NewFixedRetryPolicyWithConfig(reliability.FixedRetryConfig{
			MaxRetries: 2,
			Delay:      1000 * time.Millisecond,
		}),
		nil, // dlqHandler mặc định
		nil, // workerPool mặc định
		failingHandler{},
	)
	must(sub.Start(ctx), "start orders subscriber")

	// watcher DLQ
	watch := client.Subscriber(ctx,
		mq.SubscriberConfig{Queue: "dlq.v1", Tag: "dlq-watcher", AutoAck: false, PrefetchCount: 1},
		reliability.NewFixedRetryPolicyWithConfig(reliability.FixedRetryConfig{
			MaxRetries: 1,
			Delay:      1000 * time.Millisecond,
		}),
		nil,
		nil,
		dlqWatcher{},
	)
	must(watch.Start(ctx), "start dlq watcher")

	// publish 1 message
	pub := client.Publisher(mq.PublisherConfig{Exchange: "demo", RoutingKey: "orders"})
	msg := delivery.NewDM()
	msg.Body = []byte("order-1")
	msg.ContentType = "text/plain"
	must(pub.Publish(ctx, msg), "publish")
	log.Printf("=== published id=%s ; chờ quan sát retry (fixed 1s) rồi DLQ ===", msg.ID)

	time.Sleep(12 * time.Second)
	log.Println("=== Kết thúc ===")
}

func declareAndBind(ctx context.Context, client mq.MQ, queue, exchange, rk string) {
	_, err := client.DeclareQueue(ctx, topology.QueueConfig{Name: queue, Durable: true})
	must(err, "declare queue "+queue)
	must(client.CreateBinding(ctx, topology.BindingConfig{Queue: queue, Exchange: exchange, RoutingKey: rk}), "bind "+queue)
}

func must(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
