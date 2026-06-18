package worker

import "context"

type Job interface {
	Execute(ctx context.Context) error
}

type TaskFunc func(ctx context.Context) error

func (f TaskFunc) Execute(ctx context.Context) error {
	return f(ctx)
}

func NewTask(fn func(ctx context.Context) error) Job {
	return TaskFunc(fn)
}
