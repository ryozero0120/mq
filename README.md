# mq

**English** | [Tiếng Việt](README.vi.md)

A [RabbitMQ](https://www.rabbitmq.com/) (AMQP 0-9-1) message queue library for Go. It wraps the parts you usually have to build yourself: **auto-reconnecting connections**, a **channel pool**, **publisher/subscriber** with middleware, **retry with backoff via delay queues**, a **dead-letter queue (DLQ)**, and a **worker pool** for concurrent processing.

```
go get github.com/ryozero0120/mq
```

Requires Go 1.23+ and a RabbitMQ broker.

## Architecture

| Package | Role |
|---|---|
| `mq` | Facade: create client, publisher, subscriber, declare topology. |
| `connection` | Manages `*amqp.Connection`, auto-reconnects (exponential backoff), notifies observers. |
| `channel` | Channel pool: `Acquire`/`Release`, `Max` cap, recovers channels after reconnect. |
| `topology` | Declare exchange / queue / binding. |
| `reliability` | Retry policies (exponential, fixed), ack manager, DLQ handler. |
| `delivery` | The message type `DM` (domain message). |
| `worker` | Worker pool for concurrent delivery processing. |
| `observability` | The `Logger` interface. |

## Quick start

### Create a client

```go
ctx := context.Background()

client, err := mq.NewMQ(ctx, mq.MqConfig{
    Connection: connection.ConnectionConfig{
        URL:       "amqp://guest:guest@localhost:5672/",
        Heartbeat: 10 * time.Second,
    },
    Pool: channel.PoolConfig{Min: 2, Max: 10},
}, logger) // logger implements observability.Logger
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### Declare topology

```go
client.DeclareExchange(ctx, topology.ExchangeConfig{Name: "orders", Type: "direct", Durable: true})
client.DeclareQueue(ctx, topology.QueueConfig{Name: "orders.q", Durable: true})
client.CreateBinding(ctx, topology.BindingConfig{Queue: "orders.q", Exchange: "orders", RoutingKey: "created"})
```

### Publish

```go
pub := client.Publisher(mq.PublisherConfig{Exchange: "orders", RoutingKey: "created"})

msg := delivery.NewDM()
msg.Body = []byte(`{"id":1}`)
msg.ContentType = "application/json"

if err := pub.Publish(ctx, msg); err != nil {
    log.Printf("publish: %v", err)
}
```

### Subscribe

```go
type OrderHandler struct{}

func (OrderHandler) Handle(ctx context.Context, m *delivery.DM) error {
    // return an error to trigger retry / DLQ
    return process(m.Body)
}

sub := client.Subscriber(ctx,
    mq.SubscriberConfig{
        Queue:         "orders.q",
        Tag:           "orders-consumer",
        AutoAck:       false,
        Concurrency:   5,   // number of concurrent workers
        PrefetchCount: 20,
        RequeueOnNack: false,
    },
    nil, // retryPolicy: nil = default (fixed, 3 times, 1s)
    nil, // dlqHandler: nil = DLQ disabled (message dropped after retries exhausted)
    OrderHandler{},
)

if err := sub.Start(ctx); err != nil {
    log.Fatal(err)
}
defer sub.Stop()
```

## Retry + DLQ (delay-queue pattern)

When the handler returns an `error`, the subscriber decides whether to retry or dead-letter based on the `RetryPolicy`:

```
                handler error, retries left                 x-message-ttl expires
  [ orders.q ] ───────────────────────► [ orders.q.delay.{ttl} ] ──DLX──► [ orders.q ]
       │        (re-publish copy, x-retry-count+1)      (broker holds it, workers not blocked)
       │
       └── retries exhausted ──► DLQ handler publishes to "dlq" ──► [ dlq.v1 ] , ack original
```

Key points:

- **No `Nack(requeue)`**: on each retry the subscriber **re-publishes a copy** to a wait queue `"{queue}.delay.{ttl}"` (with `x-message-ttl` = delay, `x-dead-letter-exchange` pointing back to the source queue, `x-expires` for self-cleanup), then **acks** the original. The broker holds the message in the delay queue until the TTL expires, then dead-letters it back → **real backoff without blocking workers**.
- **Attempt count via the `x-retry-count` header** — it persists correctly because we own the header when re-publishing.
- Retries exhausted → if a `dlqHandler` is set, publish the message to the DLQ exchange and ack; if `dlqHandler = nil`, the message is acked and dropped.

### Retry policy

```go
// Exponential: delay = InitialDelay * Multiplier^attempt (capped at MaxDelay)
retry := reliability.NewExponentialRetryPolicyWithConfig(reliability.ExponentialRetryConfig{
    MaxRetries:   3,
    InitialDelay: 1 * time.Second,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
})

// Fixed: constant delay per retry
retry := reliability.NewFixedRetryPolicyWithConfig(reliability.FixedRetryConfig{
    MaxRetries: 3,
    Delay:      1 * time.Second,
})
```

`retryPolicy = nil` uses the default policy (fixed 3×1s). `dlqHandler = nil` **disables** DLQ; to dead-letter, pass a `reliability.DLQExecutor` (e.g. `reliability.NewDLQHandler(pool, reliability.DLQConfig{Exchange: "dlq", Queue: "dlq.v1", RoutingKey: "dlq"})`).

## Configuration

**`SubscriberConfig`**

| Field | Meaning |
|---|---|
| `Queue` | Queue to consume. |
| `Tag` | Consumer tag. |
| `AutoAck` | `true` = broker auto-acks (no retry/DLQ). |
| `PrefetchCount` / `PrefetchSize` | QoS prefetch. |
| `Concurrency` | Number of concurrent workers. |
| `RequeueOnNack` | Requeue flag for the fallback `Nack` path. |

**`connection.ConnectionConfig`**: `URL`, `Heartbeat`.
**`channel.PoolConfig`**: `Min` (pre-warm), `Max` (channel cap).

## Logger

Provide an implementation of `observability.Logger`:

```go
type Logger interface {
    Debug(msg string, fields ...interface{})
    Info(msg string, fields ...interface{})
    Warn(msg string, fields ...interface{})
    Error(msg string, fields ...interface{})
}
```

## Reconnect

`connection` reconnects automatically when the connection drops (exponential backoff, unlimited attempts). The channel pool and subscribers register as observers, so after a reconnect: the pool flushes dead channels and re-warms, and subscribers resume consuming.

## Run the example

```bash
docker run --rm -p 5672:5672 rabbitmq:3
go run ./examples/retrydlq/   # retry + DLQ demo
```

See [examples/retrydlq/main.go](examples/retrydlq/main.go) for a full retry-backoff → dead-letter example.

## Notes

- Publisher/subscriber middleware is available via `Use(...)`.
- Delay queues are created lazily on demand and self-delete when idle (`x-expires`).
