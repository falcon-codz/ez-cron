package metrics

import (
	"sync"
	"time"
)

// Metrics tracks execution statistics for a single job.
type Metrics struct {
	mu sync.Mutex

	TotalRuns     int64
	SuccessCount  int64
	FailureCount  int64
	LastRunAt     time.Time
	LastError     error
	TotalDuration time.Duration
}

// New creates a fresh Metrics instance.
func New() *Metrics {
	return &Metrics{}
}

// RecordSuccess records a successful job execution with its duration.
func (m *Metrics) RecordSuccess(duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalRuns++
	m.SuccessCount++
	m.LastRunAt = time.Now()
	m.TotalDuration += duration
}

// RecordFailure records a failed job execution with its duration and error.
func (m *Metrics) RecordFailure(duration time.Duration, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalRuns++
	m.FailureCount++
	m.LastRunAt = time.Now()
	m.LastError = err
	m.TotalDuration += duration
}

// AverageDuration returns the average execution duration across all runs.
func (m *Metrics) AverageDuration() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.TotalRuns == 0 {
		return 0
	}
	return m.TotalDuration / time.Duration(m.TotalRuns)
}

// Reset clears all recorded metrics.
func (m *Metrics) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.TotalRuns = 0
	m.SuccessCount = 0
	m.FailureCount = 0
	m.LastRunAt = time.Time{}
	m.LastError = nil
	m.TotalDuration = 0
}

// Snapshot returns a point-in-time copy of the metrics, safe to read without locks.
func (m *Metrics) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	avg := time.Duration(0)
	if m.TotalRuns > 0 {
		avg = m.TotalDuration / time.Duration(m.TotalRuns)
	}

	return Snapshot{
		TotalRuns:       m.TotalRuns,
		SuccessCount:    m.SuccessCount,
		FailureCount:    m.FailureCount,
		LastRunAt:       m.LastRunAt,
		LastError:       m.LastError,
		TotalDuration:   m.TotalDuration,
		AverageDuration: avg,
	}
}

// Snapshot is an immutable, point-in-time copy of job metrics.
type Snapshot struct {
	TotalRuns       int64
	SuccessCount    int64
	FailureCount    int64
	LastRunAt       time.Time
	LastError       error
	TotalDuration   time.Duration
	AverageDuration time.Duration
}
