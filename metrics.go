package ezcron

import "time"

// Metrics is a point-in-time snapshot of a job's execution statistics.
type Metrics struct {
	TotalRuns     int64
	Successes     int64
	Failures      int64
	LastRunAt     time.Time
	LastError     error
	TotalDuration time.Duration
	AvgDuration   time.Duration
}

// jobMetrics is the internal mutable metrics tracker, protected by Scheduler.mu.
type jobMetrics struct {
	totalRuns     int64
	successes     int64
	failures      int64
	lastRunAt     time.Time
	lastError     error
	totalDuration time.Duration
}

func (m *jobMetrics) record(d time.Duration, err error) {
	m.totalRuns++
	if err != nil {
		m.failures++
		m.lastError = err
	} else {
		m.successes++
	}
	m.lastRunAt = time.Now()
	m.totalDuration += d
}

func (m *jobMetrics) snapshot() Metrics {
	var avg time.Duration
	if m.totalRuns > 0 {
		avg = m.totalDuration / time.Duration(m.totalRuns)
	}
	return Metrics{
		TotalRuns:     m.totalRuns,
		Successes:     m.successes,
		Failures:      m.failures,
		LastRunAt:     m.lastRunAt,
		LastError:     m.lastError,
		TotalDuration: m.totalDuration,
		AvgDuration:   avg,
	}
}
