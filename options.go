package ezcron

import "time"

// JobOption configures a job registered with Scheduler.Add.
type JobOption func(*jobOptions)

type jobOptions struct {
	timeout    time.Duration
	maxRuns    int
	runUntil   time.Time
	stopWhen   func() bool
	tags       []string
	middleware []Middleware
}

// WithTimeout sets the maximum execution time for each run of the job.
// If the timeout elapses, the context passed to the job function is cancelled.
func WithTimeout(d time.Duration) JobOption {
	return func(o *jobOptions) { o.timeout = d }
}

// WithMaxRuns stops the job after n executions.
func WithMaxRuns(n int) JobOption {
	return func(o *jobOptions) { o.maxRuns = n }
}

// WithRunUntil stops the job after the given wall-clock time.
func WithRunUntil(t time.Time) JobOption {
	return func(o *jobOptions) { o.runUntil = t }
}

// WithStopWhen stops the job when fn returns true (checked after each run).
func WithStopWhen(fn func() bool) JobOption {
	return func(o *jobOptions) { o.stopWhen = fn }
}

// WithTags attaches tags to a job for grouping and filtering.
func WithTags(tags ...string) JobOption {
	return func(o *jobOptions) { o.tags = tags }
}

// WithMiddleware wraps the job function with the given middleware.
// Middleware is applied in the order provided (first middleware is outermost).
func WithMiddleware(mw ...Middleware) JobOption {
	return func(o *jobOptions) { o.middleware = append(o.middleware, mw...) }
}

func (o *jobOptions) shouldStop(runs int64, now time.Time) bool {
	if o.maxRuns > 0 && runs >= int64(o.maxRuns) {
		return true
	}
	if !o.runUntil.IsZero() && now.After(o.runUntil) {
		return true
	}
	if o.stopWhen != nil && o.stopWhen() {
		return true
	}
	return false
}
