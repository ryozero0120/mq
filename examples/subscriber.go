package main

import (
	"context"
	"fmt"
	"kit/mq"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type CdrHandler struct{}

func (h *CdrHandler) Handle(ctx context.Context, m *mq.Message) error {
	return nil
}

func main() {
	client, err := mq.NewMQ(mq.MqConfig{
		Connection: mq.ConnectionConfig{
			URL:       "amqp://guest:guest@localhost:5672/",
			Heartbeat: 10 * time.Second,
		},
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	defer client.Close()

	log.Printf("runtime.NumCPU(): %d", runtime.NumCPU())

	subscriber := client.Subscriber(mq.SubscriberConfig{
		Queue:         "cdr.v1",
		Tag:           fmt.Sprintf("C1_%s", time.Now().String()),
		AutoAck:       true,
		Concurrency:   runtime.NumCPU(),
		PrefetchCount: runtime.NumCPU() * 2,
	})

	subscriber.Subscribe(context.Background(), &CdrHandler{})
	if err := subscriber.Start(context.Background()); err != nil {
		log.Fatal(err.Error())
	}
	defer subscriber.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
