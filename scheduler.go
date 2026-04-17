package ezcron

import (
	"context"
	"errors"
	"sync"
	"time"
)

// JobFunc is the function signature for a job. It receives a context that is
// cancelled when the scheduler stops or the per-job timeout elapses.
type JobFunc func(ctx context.Context) error

type jobState int

const (
	stateIdle jobState = iota
	stateRunning
	statePaused
	stateStopped
)

func (s jobState) String() string {
	switch s {
	case stateIdle:
		return "idle"
	case stateRunning:
		return "running"
	case statePaused:
		return "paused"
	case stateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

type jobEntry struct {
	name     string
	fn       JobFunc
	schedule Schedule
	opts     jobOptions

	// Protected by Scheduler.mu.
	state   jobState
	gen     uint64
	metrics jobMetrics
	cancel  context.CancelFunc
	doneCh  chan struct{}
}

// JobInfo is a read-only view of a registered job's state and metrics.
type JobInfo struct {
	Name    string
	Tags    []string
	State   string
	Metrics Metrics
}

// Scheduler manages and runs jobs according to their schedules.
type Scheduler struct {
	mu       sync.Mutex
	jobs     map[string]*jobEntry
	order    []string // insertion order
	started  bool
	stopped  bool

	ctx      context.Context
	cancel   context.CancelFunc
	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New returns a new Scheduler ready to accept jobs.
func New() *Scheduler {
	return &Scheduler{
		jobs: make(map[string]*jobEntry),
	}
}

// Add registers a job. Jobs can be added before or after Start.
// The name must be unique and non-empty.
func (s *Scheduler) Add(name string, fn JobFunc, schedule Schedule, opts ...JobOption) error {
	if name == "" {
		return errors.New("ezcron: job name must not be empty")
	}
	if fn == nil {
		return errors.New("ezcron: job function must not be nil")
	}
	if schedule == nil {
		return errors.New("ezcron: schedule must not be nil")
	}

	var o jobOptions
	for _, opt := range opts {
		opt(&o)
	}

	// Apply middleware: first in slice wraps outermost.
	wrapped := fn
	for i := len(o.middleware) - 1; i >= 0; i-- {
		wrapped = o.middleware[i](wrapped)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.jobs[name]; ok {
		return ErrJobExists
	}

	e := &jobEntry{
		name:     name,
		fn:       wrapped,
		schedule: schedule,
		opts:     o,
		state:    stateIdle,
	}

	s.jobs[name] = e
	s.order = append(s.order, name)

	if s.started && !s.stopped {
		s.startJobLocked(e)
	}

	return nil
}

// Remove unregisters a job. If the job is currently running its goroutine is
// cancelled and waited on before Remove returns.
func (s *Scheduler) Remove(name string) error {
	s.mu.Lock()
	e, ok := s.jobs[name]
	if !ok {
		s.mu.Unlock()
		return ErrJobNotFound
	}

	cancel := e.cancel
	doneCh := e.doneCh

	delete(s.jobs, name)
	for i, n := range s.order {
		if n == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
		<-doneCh
	}

	return nil
}

// Start launches goroutines for all registered jobs. The provided context acts
// as the parent for every job context; cancelling it is equivalent to calling Stop.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.stopCh = make(chan struct{})
	s.stopOnce = sync.Once{}
	s.started = true
	s.stopped = false

	for _, name := range s.order {
		s.startJobLocked(s.jobs[name])
	}
}

func (s *Scheduler) startJobLocked(e *jobEntry) {
	jobCtx, jobCancel := context.WithCancel(s.ctx)
	e.cancel = jobCancel
	e.doneCh = make(chan struct{})
	e.state = stateRunning
	e.gen++
	gen := e.gen

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(e.doneCh)
		s.runJob(jobCtx, e, gen)
	}()
}

func (s *Scheduler) runJob(ctx context.Context, e *jobEntry, gen uint64) {
	for {
		next := e.schedule.Next(time.Now())
		if next.IsZero() {
			return
		}

		if wait := time.Until(next); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-s.stopCh:
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		// Verify this goroutine is still the active one for its job.
		s.mu.Lock()
		if e.gen != gen {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		// Execute with optional per-job timeout.
		execCtx := ctx
		var execCancel context.CancelFunc
		if e.opts.timeout > 0 {
			execCtx, execCancel = context.WithTimeout(ctx, e.opts.timeout)
		}

		start := time.Now()
		err := e.fn(execCtx)
		if execCancel != nil {
			execCancel()
		}
		dur := time.Since(start)

		// Record metrics and check stop conditions.
		s.mu.Lock()
		if e.gen != gen {
			s.mu.Unlock()
			return
		}
		e.metrics.record(dur, err)

		if e.opts.shouldStop(e.metrics.totalRuns, time.Now()) {
			e.state = stateStopped
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()
	}
}

// Shutdown gracefully stops the scheduler. It signals all job goroutines to
// stop scheduling new runs, then waits for in-flight executions to complete.
// If the provided context expires before all jobs finish, it hard-cancels them
// and returns the context error.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.started || s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	s.mu.Unlock()

	// Signal goroutines to stop picking up new runs.
	s.stopOnce.Do(func() { close(s.stopCh) })

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.cancel()
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return nil
	case <-ctx.Done():
		s.cancel() // hard cancel
		<-done     // wait for goroutines to exit
		s.mu.Lock()
		s.started = false
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Stop immediately cancels all running jobs and blocks until their goroutines exit.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.started || s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	s.stopOnce.Do(func() { close(s.stopCh) })
	s.cancel()
	s.wg.Wait()

	s.mu.Lock()
	s.started = false
	s.mu.Unlock()
}

// PauseJob pauses a running job. Its goroutine is cancelled and waited on.
// Call ResumeJob to restart it.
func (s *Scheduler) PauseJob(name string) error {
	s.mu.Lock()
	e, ok := s.jobs[name]
	if !ok {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if e.state != stateRunning {
		s.mu.Unlock()
		return ErrJobNotRunning
	}

	e.state = statePaused
	e.gen++
	cancel := e.cancel
	doneCh := e.doneCh
	s.mu.Unlock()

	cancel()
	<-doneCh

	return nil
}

// ResumeJob resumes a paused job by launching a new goroutine.
func (s *Scheduler) ResumeJob(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.jobs[name]
	if !ok {
		return ErrJobNotFound
	}
	if e.state != statePaused {
		return ErrJobNotPaused
	}

	s.startJobLocked(e)
	return nil
}

// Jobs returns a snapshot of all registered jobs in insertion order.
func (s *Scheduler) Jobs() []JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	infos := make([]JobInfo, 0, len(s.order))
	for _, name := range s.order {
		e := s.jobs[name]
		tags := make([]string, len(e.opts.tags))
		copy(tags, e.opts.tags)
		infos = append(infos, JobInfo{
			Name:    e.name,
			Tags:    tags,
			State:   e.state.String(),
			Metrics: e.metrics.snapshot(),
		})
	}
	return infos
}

// JobMetrics returns the current metrics for the named job.
func (s *Scheduler) JobMetrics(name string) (Metrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.jobs[name]
	if !ok {
		return Metrics{}, ErrJobNotFound
	}
	return e.metrics.snapshot(), nil
}
