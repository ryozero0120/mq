package worker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Pool interface {
	Start()
	Stop()
	Submit(job Job) error
	Size() int
}

type Config struct {
	Workers         int
	QueueSize       int
	ShutdownTimeout time.Duration
	JobTimeout      time.Duration
	OnJobError      func(err error)
}

type WorkerPool struct {
	config Config

	jobQueue chan Job
	done     chan struct{}
	wg       sync.WaitGroup

	mu sync.RWMutex
}

func NewWorkerPool(config Config) *WorkerPool {
	if config.Workers <= 0 {
		config.Workers = 1
	}

	if config.QueueSize <= 0 {
		config.QueueSize = 1
	}

	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 30 * time.Second
	}

	return &WorkerPool{
		config:   config,
		jobQueue: make(chan Job, config.QueueSize),
		done:     make(chan struct{}),
	}
}

func (p *WorkerPool) Start() {
	p.mu.RLock()
	defer p.mu.RUnlock()

	p.done = make(chan struct{})

	for i := 0; i < p.config.Workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *WorkerPool) Stop() {
	p.mu.RLock()
	done := p.done
	defer p.mu.RUnlock()

	close(done)

	finished := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
		fmt.Print("Workerpool: finished")
	case <-time.After(p.config.ShutdownTimeout):
		fmt.Print("Workerpool: shutdown timeout")
	}
}

func (p *WorkerPool) Submit(j Job) error {
	if j == nil {
		return fmt.Errorf("task cannot be nil")
	}

	select {
	case p.jobQueue <- j:
		return nil
	default:
		return fmt.Errorf("task queue is full (size: %d)", p.config.QueueSize)
	}
}

func (p *WorkerPool) Size() int {
	return p.config.Workers
}

func (p *WorkerPool) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.done:
			// Drain remaining tasks before exiting for graceful shutdown
			p.drainQueue()
			return
		case job, ok := <-p.jobQueue:
			if !ok {
				return
			}
			p.executeJob(job)
		}
	}
}

func (p *WorkerPool) executeJob(job Job) {
	ctx := context.Background()
	var cancel context.CancelFunc

	if p.config.JobTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, p.config.JobTimeout*time.Second)
		defer cancel()
	}

	err := job.Execute(ctx)
	if err != nil {
		return
	}
}

func (p *WorkerPool) drainQueue() {
	for {
		select {
		case job, ok := <-p.jobQueue:
			if !ok {
				return
			}

			p.executeJob(job)
		default:
			return
		}
	}
}
