package retry

import (
	"errors"
	"time"
)

type NonRetryableError struct {
	Err error
}

func (e NonRetryableError) Error() string {
	if e.Err == nil {
		return "non-retryable error"
	}
	return e.Err.Error()
}

func (e NonRetryableError) Unwrap() error {
	return e.Err
}

func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return NonRetryableError{Err: err}
}

func IsRetryable(err error) bool {
	if err == nil {
		return true
	}
	var nr NonRetryableError
	return !errors.As(err, &nr)
}

type Policy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func DefaultPolicy() Policy {
	return Policy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  10 * time.Minute,
	}
}

func (p Policy) Delay(retryCount int) time.Duration {
	if retryCount < 1 {
		retryCount = 1
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 1 * time.Second
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 10 * time.Minute
	}

	delay := p.BaseDelay * time.Duration(1<<uint(retryCount-1))
	if delay > p.MaxDelay {
		return p.MaxDelay
	}
	return delay
}

func (p Policy) NextAvailableAt(retryCount int, now time.Time) time.Time {
	return now.UTC().Add(p.Delay(retryCount))
}