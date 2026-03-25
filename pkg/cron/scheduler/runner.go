package scheduler

import (
	"context"
	"sync"
	"time"

	"github.com/falcon-codz/ez-cron/pkg/cron/job"
)

// runJob executes a job in a loop: wait for interval, run command, record metrics, check trigger.
// Exits when ctx is cancelled or when the job's trigger signals stop.
func runJob(ctx context.Context, j *job.Job, wg *sync.WaitGroup) {
	defer wg.Done()

	if j.Interval <= 0 {
		return
	}

	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	var runCount int

	for {
		select {
		case <-ctx.Done():
			j.SetState(job.Stopped)
			return
		case <-ticker.C:
			if j.GetState() == job.Paused {
				continue
			}

			j.SetState(job.Running)
			start := time.Now()
			err := j.Command.Execute()
			duration := time.Since(start)

			if err != nil {
				j.Metrics.RecordFailure(duration, err)
			} else {
				j.Metrics.RecordSuccess(duration)
			}

			runCount++

			if j.Trigger != nil && j.Trigger.ShouldStop(runCount) {
				j.SetState(job.Completed)
				return
			}

			j.SetState(job.Idle)
		}
	}
}
