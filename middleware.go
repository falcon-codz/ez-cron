package ezcron

import (
	"context"
	"fmt"
	"time"
)

// Middleware wraps a JobFunc to add cross-cutting behaviour.
// The first middleware passed to WithMiddleware is the outermost wrapper.
type Middleware func(JobFunc) JobFunc

// Recover catches panics in the job function and converts them to errors.
func Recover() Middleware {
	return func(next JobFunc) JobFunc {
		return func(ctx context.Context) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			return next(ctx)
		}
	}
}

// Retry re-executes the job function up to attempts times on failure.
// It stops retrying early if the context is cancelled.
func Retry(attempts int) Middleware {
	if attempts < 1 {
		attempts = 1
	}
	return func(next JobFunc) JobFunc {
		return func(ctx context.Context) error {
			var err error
			for i := 0; i < attempts; i++ {
				if err = next(ctx); err == nil {
					return nil
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Simple linear back-off: 100ms * attempt number.
				if i < attempts-1 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(100 * time.Millisecond * time.Duration(i+1)):
					}
				}
			}
			return err
		}
	}
}

// LogErrors calls fn whenever the wrapped job returns a non-nil error.
func LogErrors(fn func(err error)) Middleware {
	return func(next JobFunc) JobFunc {
		return func(ctx context.Context) error {
			err := next(ctx)
			if err != nil {
				fn(err)
			}
			return err
		}
	}
}
