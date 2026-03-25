package trigger

// Trigger determines whether a job should stop executing.
type Trigger interface {
	ShouldStop(runCount int) bool
}

// RunNTimes stops a job after it has executed N times.
type RunNTimes struct {
	N int
}

// NewRunNTimes creates a trigger that stops after n executions.
func NewRunNTimes(n int) *RunNTimes {
	return &RunNTimes{N: n}
}

// ShouldStop returns true when runCount has reached the configured limit.
func (r *RunNTimes) ShouldStop(runCount int) bool {
	return runCount >= r.N
}

// RunUntil stops a job when the provided condition function returns true.
type RunUntil struct {
	Condition func() bool
}

// NewRunUntil creates a trigger that stops when condition returns true.
func NewRunUntil(condition func() bool) *RunUntil {
	return &RunUntil{Condition: condition}
}

// ShouldStop returns true when the condition is met.
func (r *RunUntil) ShouldStop(_ int) bool {
	return r.Condition()
}
