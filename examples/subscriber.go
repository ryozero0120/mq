package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/ryozero0120/mq"
	"github.com/ryozero0120/mq/channel"
	"github.com/ryozero0120/mq/connection"
	"github.com/ryozero0120/mq/delivery"
	"github.com/ryozero0120/mq/topology"
)

type CdrHandler struct{}

func (h *CdrHandler) Handle(ctx context.Context, m *delivery.DM) error {
	return nil
}

func main() {
	log.Print("ok")
	ctx, cancel := context.WithCancel(context.Background())

	client, err := mq.NewMQ(
		ctx,
		mq.MqConfig{
			Connection: connection.ConnectionConfig{
				URL:       "amqp://guest:guest@localhost:5672/",
				Heartbeat: 10 * time.Second,
			},
			Pool: channel.PoolConfig{
				Min:            5,
				Max:            20,
				IdleTimeout:    30 * time.Second,
				MaxLifetime:    5 * time.Minute,
				AcquireTimeout: 5 * time.Second,
			},
		},
		nil)

	log.Print("ok")

	if err != nil {
		log.Fatal(err.Error())
	}

	defer client.Close()

	err = client.DeclareExchange(ctx, topology.ExchangeConfig{
		Name:       "cdr",
		Type:       "direct",
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Arguments:  map[string]interface{}{},
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	queue, err := client.DeclareQueue(ctx, topology.QueueConfig{
		Name:       "cdr.v3",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Arguments: map[string]interface{}{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	})

	if err != nil {
		log.Fatal(err.Error())
	}

	client.CreateBinding(ctx, topology.BindingConfig{
		Queue:      queue.Name,
		Exchange:   "cdr",
		RoutingKey: "cdr.v3",
		NoWait:     false,
		Arguments:  map[string]interface{}{},
	})

	log.Printf("runtime.NumCPU(): %d", runtime.NumCPU())

	hanlder := &CdrHandler{}

	subscriber := client.Subscriber(
		ctx,
		mq.SubscriberConfig{
			Queue:         queue.Name,
			Tag:           fmt.Sprintf("C1_%s", time.Now().String()),
			AutoAck:       false,
			PrefetchCount: runtime.NumCPU() * 8,
			RequeueOnNack: true,
		},
		nil, // retry policy
		nil, // dlqHandler
		nil, // workerPool
		hanlder,
	)

	if err := subscriber.Start(ctx); err != nil {
		log.Fatal(err.Error())
	}
	defer subscriber.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	cancel()
}
