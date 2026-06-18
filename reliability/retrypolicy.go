package reliability

import (
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

type RetryPolicy interface {
	ShouldRetry() bool
	NextDelay(attempt int) time.Duration
	MaxAttempts() int
}

type ExponentialRetryPolicy struct {
	maxRetries      int
	initialDelay    time.Duration
	maxDelay        time.Duration
	multiplier      float64
	jitterFraction  float64
	retryableErrors []string
}

type ExponentialRetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	Multiplier      float64
	JitterFraction  float64
	RetryableErrors []string
}

func NewExponentialRetryPolicyWithConfig(config ExponentialRetryConfig) *ExponentialRetryPolicy {
	jitter := config.JitterFraction
	if jitter == 0 {
		jitter = 0.2
	}

	retryableErrors := config.RetryableErrors
	if len(retryableErrors) == 0 {
		retryableErrors = []string{
			"timeout",
			"temporary",
			"connection refused",
			"connection reset",
			"broken pipe",
			"no such host",
			"service unavailable",
			"too many requests",
			"rate limit",
		}
	}

	return &ExponentialRetryPolicy{
		maxRetries:      config.MaxRetries,
		initialDelay:    config.InitialDelay,
		maxDelay:        config.MaxDelay,
		jitterFraction:  jitter,
		retryableErrors: retryableErrors,
	}
}

func (p *ExponentialRetryPolicy) ShouldRetry(err error, attempt int) bool {
	if err == nil {
		return false
	}

	if attempt > p.maxRetries {
		return false
	}

	// Check if error is retryable
	return p.isRetryableError(err)
}

// NextDelay calculates the next retry delay with exponential backoff and jitter
func (p *ExponentialRetryPolicy) NextDelay(attempt int) time.Duration {
	// Calculate exponential delay: initialDelay * (multiplier ^ attempt)
	delay := float64(p.initialDelay) * math.Pow(p.multiplier, float64(attempt))

	// Apply jitter (random ±jitterFraction)
	jitter := (rand.Float64()*2 - 1) * p.jitterFraction // Range: [-jitterFraction, +jitterFraction]
	delay = delay * (1 + jitter)

	// Cap at max delay
	if delay > float64(p.maxDelay) {
		delay = float64(p.maxDelay)
	}

	// Ensure minimum delay
	if delay < float64(p.initialDelay) {
		delay = float64(p.initialDelay)
	}

	return time.Duration(delay)
}

func (p *ExponentialRetryPolicy) MaxAttempts() int {
	return p.maxRetries
}

func (p *ExponentialRetryPolicy) isRetryableError(err error) bool {
	errMsg := strings.ToLower(err.Error())
	for _, kw := range p.retryableErrors {
		if strings.Contains(errMsg, kw) {
			return true
		}
	}
	return true
}
