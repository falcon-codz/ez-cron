package scheduler

import (
	"context"
	"errors"
	"sync"

	"github.com/falcon-codz/ez-cron/pkg/cron/job"
)

var (
	ErrJobExists        = errors.New("job with this ID already exists")
	ErrJobNotFound      = errors.New("job not found")
	ErrJobNotRunning    = errors.New("job is not running")
	ErrSchedulerRunning = errors.New("cannot modify jobs while scheduler is running; stop the scheduler first")
)

type runState struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Scheduler holds a registry of jobs and runs each in its own goroutine.
type Scheduler struct {
	mu      sync.RWMutex
	jobs    map[string]*job.Job
	active  map[string]*runState
	ctx     context.Context
	cancel  context.CancelFunc
	running bool
}

// New returns a new Scheduler with an empty job registry.
func New() *Scheduler {
	return &Scheduler{
		jobs: make(map[string]*job.Job),
	}
}

// AddJob registers a job. Returns ErrJobExists if a job with the same ID is already registered.
func (s *Scheduler) AddJob(j *job.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerRunning
	}
	if _, ok := s.jobs[j.ID]; ok {
		return ErrJobExists
	}
	s.jobs[j.ID] = j
	return nil
}

// RemoveJob removes a job by ID. Returns ErrJobNotFound or ErrSchedulerRunning if the scheduler is running.
func (s *Scheduler) RemoveJob(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return ErrSchedulerRunning
	}
	if _, ok := s.jobs[id]; !ok {
		return ErrJobNotFound
	}
	delete(s.jobs, id)
	return nil
}

// Start starts a goroutine for each registered job. Cancelling ctx or calling Stop() stops all jobs.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.active = make(map[string]*runState)
	s.running = true
	for id, j := range s.jobs {
		s.startJobLocked(id, j)
	}
	s.mu.Unlock()
}

// startJobLocked starts one job's goroutine. Caller must hold s.mu.
func (s *Scheduler) startJobLocked(id string, j *job.Job) {
	jobCtx, cancel := context.WithCancel(s.ctx)
	rs := &runState{cancel: cancel}
	rs.wg.Add(1)
	s.active[id] = rs
	go runJob(jobCtx, j, &rs.wg)
}

// Stop cancels all job goroutines and blocks until they have exited.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	cancel := s.cancel
	activeCopy := make(map[string]*runState, len(s.active))
	for k, v := range s.active {
		activeCopy[k] = v
	}
	s.active = nil
	s.mu.Unlock()

	cancel()
	for _, rs := range activeCopy {
		rs.wg.Wait()
	}
}

// StopJob stops a single job's goroutine and blocks until it has exited.
func (s *Scheduler) StopJob(id string) error {
	s.mu.Lock()
	j, inJobs := s.jobs[id]
	rs, inActive := s.active[id]
	if !inJobs {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if !inActive {
		s.mu.Unlock()
		return ErrJobNotRunning
	}
	rs.cancel()
	delete(s.active, id)
	s.mu.Unlock()

	rs.wg.Wait()
	j.SetState(job.Stopped)
	return nil
}

// PauseJob sets the job state to Paused so it stops executing until resumed.
func (s *Scheduler) PauseJob(id string) error {
	s.mu.Lock()
	j, inJobs := s.jobs[id]
	_, inActive := s.active[id]
	if !inJobs {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if !inActive {
		s.mu.Unlock()
		return ErrJobNotRunning
	}
	s.mu.Unlock()
	j.SetState(job.Paused)
	return nil
}

// ResumeJob sets the job state to Idle so it resumes executing.
func (s *Scheduler) ResumeJob(id string) error {
	s.mu.Lock()
	j, inJobs := s.jobs[id]
	_, inActive := s.active[id]
	if !inJobs {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if !inActive {
		s.mu.Unlock()
		return ErrJobNotRunning
	}
	s.mu.Unlock()
	j.SetState(job.Idle)
	return nil
}

// RestartJob stops the job's goroutine and starts it again.
func (s *Scheduler) RestartJob(id string) error {
	s.mu.Lock()
	j, inJobs := s.jobs[id]
	if !inJobs {
		s.mu.Unlock()
		return ErrJobNotFound
	}
	if !s.running {
		s.mu.Unlock()
		return ErrJobNotRunning
	}
	if rs, inActive := s.active[id]; inActive {
		rs.cancel()
		delete(s.active, id)
		s.mu.Unlock()
		rs.wg.Wait()
		j.SetState(job.Stopped)
		s.mu.Lock()
		if !s.running {
			s.mu.Unlock()
			return nil
		}
	}
	j.SetState(job.Idle)
	s.startJobLocked(id, j)
	s.mu.Unlock()
	return nil
}

// GetJob returns the job with the given ID and true, or nil and false if not found.
func (s *Scheduler) GetJob(id string) (*job.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	return j, ok
}

// ListJobs returns a snapshot of all registered jobs.
func (s *Scheduler) ListJobs() []*job.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*job.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}
