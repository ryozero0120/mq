# mq

[English](README.md) | **Tiếng Việt**

Thư viện message queue trên nền [RabbitMQ](https://www.rabbitmq.com/) (AMQP 0-9-1) cho Go, gói lại các phần thường phải tự viết: **connection tự reconnect**, **channel pool**, **publisher/subscriber** với middleware, **retry có backoff qua delay queue**, **dead-letter queue (DLQ)** và **worker pool** xử lý song song.

```
go get github.com/ryozero0120/mq
```

Yêu cầu: Go 1.23+ và một RabbitMQ broker.

## Kiến trúc

| Package | Vai trò |
|---|---|
| `mq` | Facade: tạo client, publisher, subscriber, khai báo topology. |
| `connection` | Quản lý `*amqp.Connection`, tự động reconnect (exponential backoff) + phát sự kiện cho observer. |
| `channel` | Channel pool: `Acquire`/`Release`, giới hạn `Max`, tự phục hồi channel sau reconnect. |
| `topology` | Khai báo exchange / queue / binding. |
| `reliability` | Retry policy (exponential, fixed), ack manager, DLQ handler. |
| `delivery` | Kiểu message `DM` (domain message). |
| `worker` | Worker pool xử lý delivery song song. |
| `observability` | Interface `Logger`. |

## Bắt đầu nhanh

### Khởi tạo client

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

### Khai báo topology

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
    // trả về error để kích hoạt retry / DLQ
    return process(m.Body)
}

sub := client.Subscriber(ctx,
    mq.SubscriberConfig{
        Queue:         "orders.q",
        Tag:           "orders-consumer",
        AutoAck:       false,
        Concurrency:   5,   // số worker xử lý song song
        PrefetchCount: 20,
        RequeueOnNack: false,
    },
    nil, // retryPolicy: nil = mặc định (fixed, 3 lần, 1s)
    nil, // dlqHandler: nil = tắt DLQ (message bị bỏ sau khi hết retry)
    OrderHandler{},
)

if err := sub.Start(ctx); err != nil {
    log.Fatal(err)
}
defer sub.Stop()
```

## Retry + DLQ (delay-queue pattern)

Khi handler trả về `error`, subscriber quyết định retry hay đẩy DLQ dựa trên `RetryPolicy`:

```
                handler error, còn retry                    hết x-message-ttl
  [ orders.q ] ───────────────────────► [ orders.q.delay.{ttl} ] ──DLX──► [ orders.q ]
       │        (re-publish copy, x-retry-count+1)      (broker giữ chỗ, không block worker)
       │
       └── hết retry ──► DLQ handler publish sang "dlq" ──► [ dlq.v1 ] , ack bản gốc
```

Điểm chính:

- **Không dùng `Nack(requeue)`**: mỗi lần retry, subscriber **re-publish một bản copy** vào wait queue `"{queue}.delay.{ttl}"` (đặt `x-message-ttl` = delay, `x-dead-letter-exchange` trỏ về queue gốc, `x-expires` để tự dọn), rồi **ack** bản gốc. Broker giữ message trong delay queue tới khi hết TTL rồi dead-letter ngược lại → **backoff thật, không block worker**.
- **Đếm attempt bằng header `x-retry-count`** — persist đúng vì ta làm chủ header khi re-publish.
- Hết `MaxRetries` → nếu có `dlqHandler`, publish message sang DLQ exchange rồi ack; nếu `dlqHandler = nil` thì message được ack và bỏ.

### Retry policy

```go
// Exponential: delay = InitialDelay * Multiplier^attempt (cap MaxDelay)
retry := reliability.NewExponentialRetryPolicyWithConfig(reliability.ExponentialRetryConfig{
    MaxRetries:   3,
    InitialDelay: 1 * time.Second,
    MaxDelay:     30 * time.Second,
    Multiplier:   2.0,
})

// Fixed: delay cố định mỗi lần
retry := reliability.NewFixedRetryPolicyWithConfig(reliability.FixedRetryConfig{
    MaxRetries: 3,
    Delay:      1 * time.Second,
})
```

`retryPolicy = nil` dùng policy mặc định (fixed 3×1s). `dlqHandler = nil` **tắt** DLQ; muốn dead-letter thì truyền vào một `reliability.DLQExecutor` (vd `reliability.NewDLQHandler(pool, reliability.DLQConfig{Exchange: "dlq", Queue: "dlq.v1", RoutingKey: "dlq"})`).

## Cấu hình

**`SubscriberConfig`**

| Field | Ý nghĩa |
|---|---|
| `Queue` | Tên queue tiêu thụ. |
| `Tag` | Consumer tag. |
| `AutoAck` | `true` = broker auto-ack (không retry/DLQ). |
| `PrefetchCount` / `PrefetchSize` | QoS prefetch. |
| `Concurrency` | Số worker xử lý song song. |
| `RequeueOnNack` | Cờ requeue cho nhánh fallback `Nack`. |

**`connection.ConnectionConfig`**: `URL`, `Heartbeat`.
**`channel.PoolConfig`**: `Min` (pre-warm), `Max` (trần channel).

## Logger

Cung cấp một implementation của `observability.Logger`:

```go
type Logger interface {
    Debug(msg string, fields ...interface{})
    Info(msg string, fields ...interface{})
    Warn(msg string, fields ...interface{})
    Error(msg string, fields ...interface{})
}
```

## Reconnect

`connection` tự động kết nối lại khi mất kết nối (exponential backoff, không giới hạn số lần). Channel pool và subscriber đăng ký observer nên sau khi reconnect: pool flush channel chết và nạp lại, subscriber consume lại.

## Chạy ví dụ

```bash
docker run --rm -p 5672:5672 rabbitmq:3
go run ./examples/retrydlq/   # demo retry + DLQ
```

Xem [examples/retrydlq/main.go](examples/retrydlq/main.go) để có ví dụ đầy đủ retry backoff → dead-letter.

## Ghi chú

- Middleware cho publisher/subscriber có sẵn qua `Use(...)`.
- Delay queue được tạo lazy khi cần và tự xoá khi idle (`x-expires`).
