// Package ezcron provides a simple API to run and manage cron jobs.
package ezcron

import (
	"context"
	"time"

	"github.com/falcon-codz/ez-cron/pkg/cron/command"
	"github.com/falcon-codz/ez-cron/pkg/cron/interval"
	"github.com/falcon-codz/ez-cron/pkg/cron/job"
	"github.com/falcon-codz/ez-cron/pkg/cron/scheduler"
	"github.com/falcon-codz/ez-cron/pkg/cron/trigger"
)

// Command is the interface that a cron job must implement.
type Command = command.Command

// Job is a scheduled unit of work. Create with NewJob.
type Job = job.Job

// Trigger determines when a job should stop. Use NewRunNTimes or NewRunUntil.
type Trigger = trigger.Trigger

// Scheduler runs jobs in separate goroutines.
type Scheduler struct {
	inner *scheduler.Scheduler
}

// New returns a new Scheduler.
func New() *Scheduler {
	return &Scheduler{inner: scheduler.New()}
}

// AddJob registers a job. Returns an error if the job ID already exists or the scheduler is already running.
func (s *Scheduler) AddJob(j *Job) error {
	return s.inner.AddJob(j)
}

// RemoveJob removes a job by ID. Scheduler must be stopped first.
func (s *Scheduler) RemoveJob(id string) error {
	return s.inner.RemoveJob(id)
}

// Start starts all registered jobs. Cancel ctx or call Stop to stop them.
func (s *Scheduler) Start(ctx context.Context) {
	s.inner.Start(ctx)
}

// Stop stops all jobs and blocks until their goroutines have exited.
func (s *Scheduler) Stop() {
	s.inner.Stop()
}

// GetJob returns the job with the given ID, or nil and false if not found.
func (s *Scheduler) GetJob(id string) (*Job, bool) {
	return s.inner.GetJob(id)
}

// ListJobs returns all registered jobs.
func (s *Scheduler) ListJobs() []*Job {
	return s.inner.ListJobs()
}

// StopJob stops a single job and blocks until its goroutine has exited.
func (s *Scheduler) StopJob(id string) error {
	return s.inner.StopJob(id)
}

// PauseJob pauses a job so it stops executing until resumed.
func (s *Scheduler) PauseJob(id string) error {
	return s.inner.PauseJob(id)
}

// ResumeJob resumes a paused job.
func (s *Scheduler) ResumeJob(id string) error {
	return s.inner.ResumeJob(id)
}

// RestartJob stops the job and starts it again.
func (s *Scheduler) RestartJob(id string) error {
	return s.inner.RestartJob(id)
}

// NewJob creates a job. Use WithTags and WithTrigger for options.
func NewJob(id, name string, cmd Command, interval time.Duration, opts ...JobOption) *Job {
	return job.New(id, name, cmd, interval, opts...)
}

// JobOption configures a job (e.g. tags, trigger).
type JobOption = job.Option

// WithTags attaches tags to a job for grouping and filtering.
func WithTags(tags ...string) JobOption {
	return job.WithTags(tags...)
}

// WithTrigger sets when the job should stop (e.g. after N runs).
func WithTrigger(t Trigger) JobOption {
	return job.WithTrigger(t)
}

// Predefined intervals.
const (
	EverySecond    = interval.EverySecond
	Every5Seconds  = interval.Every5Seconds
	Every30Seconds = interval.Every30Seconds
	EveryMinute    = interval.EveryMinute
	Every5Minutes  = interval.Every5Minutes
	Every15Minutes = interval.Every15Minutes
	Every30Minutes = interval.Every30Minutes
	EveryHour      = interval.EveryHour
	Daily          = interval.Daily
	Weekly         = interval.Weekly
)

// NewRunNTimes returns a trigger that stops the job after n runs.
func NewRunNTimes(n int) Trigger {
	return trigger.NewRunNTimes(n)
}

// NewRunUntil returns a trigger that stops the job when the condition returns true.
func NewRunUntil(condition func() bool) Trigger {
	return trigger.NewRunUntil(condition)
}

// Sentinel errors from the scheduler.
var (
	ErrJobExists        = scheduler.ErrJobExists
	ErrJobNotFound      = scheduler.ErrJobNotFound
	ErrJobNotRunning    = scheduler.ErrJobNotRunning
	ErrSchedulerRunning = scheduler.ErrSchedulerRunning
)
