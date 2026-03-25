package job

import (
	"sync"
	"time"

	"github.com/falcon-codz/ez-cron/pkg/cron/command"
	"github.com/falcon-codz/ez-cron/pkg/cron/metrics"
	"github.com/falcon-codz/ez-cron/pkg/cron/trigger"
)

// Job represents a scheduled unit of work with its configuration and metadata.
type Job struct {
	ID        string
	Name      string
	Command   command.Command
	Tags      []string
	Interval  time.Duration
	Trigger   trigger.Trigger
	Metrics   *metrics.Metrics
	CreatedAt time.Time

	mu    sync.RWMutex
	state State
}

// GetState returns the current lifecycle state of the job.
func (j *Job) GetState() State {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.state
}

// SetState updates the job state. Used by the scheduler runner.
func (j *Job) SetState(s State) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.state = s
}

// Option is a functional option for configuring a Job.
type Option func(*Job)

// New creates a Job with the given id, name, command, and interval.
// Additional configuration can be applied via functional options.
func New(id, name string, cmd command.Command, interval time.Duration, opts ...Option) *Job {
	j := &Job{
		ID:        id,
		Name:      name,
		Command:   cmd,
		Interval:  interval,
		state:     Idle,
		Tags:      []string{},
		Metrics:   metrics.New(),
		CreatedAt: time.Now(),
	}

	for _, opt := range opts {
		opt(j)
	}

	return j
}

// WithTags sets the tags on a job.
func WithTags(tags ...string) Option {
	return func(j *Job) {
		j.Tags = tags
	}
}

// WithTrigger sets the stop trigger on a job.
func WithTrigger(t trigger.Trigger) Option {
	return func(j *Job) {
		j.Trigger = t
	}
}

// HasTag returns true if the job has the given tag.
func (j *Job) HasTag(tag string) bool {
	for _, t := range j.Tags {
		if t == tag {
			return true
		}
	}
	return false
}
