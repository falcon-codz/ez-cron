package ezcron

import "time"

// Schedule determines when a job should next run.
type Schedule interface {
	Next(now time.Time) time.Time
}

type intervalSchedule struct {
	d time.Duration
}

// Every returns a Schedule that fires every d duration.
// Durations less than one millisecond are clamped to one millisecond.
func Every(d time.Duration) Schedule {
	if d < time.Millisecond {
		d = time.Millisecond
	}
	return &intervalSchedule{d: d}
}

func (s *intervalSchedule) Next(now time.Time) time.Time {
	return now.Add(s.d)
}

// Predefined durations for use with Every.
const (
	EverySecond    = time.Second
	Every5Seconds  = 5 * time.Second
	Every30Seconds = 30 * time.Second
	EveryMinute    = time.Minute
	Every5Minutes  = 5 * time.Minute
	Every15Minutes = 15 * time.Minute
	Every30Minutes = 30 * time.Minute
	EveryHour      = time.Hour
	Daily          = 24 * time.Hour
	Weekly         = 7 * 24 * time.Hour
)
