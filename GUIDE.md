# The ez-cron Guide

A complete walkthrough of the `ezcron` library — from first import to production-ready patterns.

---

## Table of Contents

1. [Installation](#1-installation)
2. [Quick Start](#2-quick-start)
3. [Core Concepts](#3-core-concepts)
   - [JobFunc](#jobfunc)
   - [Schedule](#schedule)
   - [Scheduler](#scheduler)
4. [Scheduling](#4-scheduling)
   - [Fixed Intervals with `Every`](#fixed-intervals-with-every)
   - [Predefined Interval Constants](#predefined-interval-constants)
   - [Cron Expressions with `Cron`](#cron-expressions-with-cron)
   - [Cron Syntax Reference](#cron-syntax-reference)
   - [Cron Shortcuts](#cron-shortcuts)
   - [Custom Schedules](#custom-schedules)
5. [Job Options](#5-job-options)
   - [`WithMaxRuns` — Run N Times Then Stop](#withmaxruns--run-n-times-then-stop)
   - [`WithRunUntil` — Stop After a Deadline](#withrununtil--stop-after-a-deadline)
   - [`WithStopWhen` — Stop on a Condition](#withstopwhen--stop-on-a-condition)
   - [`WithTimeout` — Per-Execution Time Limit](#withtimeout--per-execution-time-limit)
   - [`WithTags` — Tag Jobs for Grouping](#withtags--tag-jobs-for-grouping)
   - [`WithMiddleware` — Attach Middleware](#withmiddleware--attach-middleware)
   - [Combining Options](#combining-options)
6. [Middleware](#6-middleware)
   - [`Recover` — Catch Panics](#recover--catch-panics)
   - [`Retry` — Automatic Retries](#retry--automatic-retries)
   - [`LogErrors` — Error Reporting](#logerrors--error-reporting)
   - [Writing Custom Middleware](#writing-custom-middleware)
   - [Stacking Middleware](#stacking-middleware)
7. [Job Management](#7-job-management)
   - [Adding Jobs Before Start](#adding-jobs-before-start)
   - [Adding Jobs at Runtime](#adding-jobs-at-runtime)
   - [Removing Jobs](#removing-jobs)
   - [Pausing and Resuming Jobs](#pausing-and-resuming-jobs)
   - [Listing All Jobs](#listing-all-jobs)
8. [Metrics and Monitoring](#8-metrics-and-monitoring)
   - [Reading Job Metrics](#reading-job-metrics)
   - [Building a Health Endpoint](#building-a-health-endpoint)
9. [Shutdown Patterns](#9-shutdown-patterns)
   - [Immediate Stop](#immediate-stop)
   - [Graceful Shutdown](#graceful-shutdown)
   - [Signal-Based Shutdown](#signal-based-shutdown)
10. [Error Handling](#10-error-handling)
11. [Full Example Application](#11-full-example-application)
12. [API Reference](#12-api-reference)

---

## 1. Installation

```bash
go get github.com/falcon-codz/ez-cron@latest
```

Import the package:

```go
import ezcron "github.com/falcon-codz/ez-cron"
```

**Requirements**: Go 1.21 or later. Zero external dependencies.

---

## 2. Quick Start

A minimal working program that runs a job every 2 seconds and shuts down cleanly on Ctrl+C:

```go
package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	ezcron "github.com/falcon-codz/ez-cron"
)

func main() {
	s := ezcron.New()

	s.Add("hello", func(ctx context.Context) error {
		fmt.Println(time.Now().Format("15:04:05"), "— hello from ez-cron!")
		return nil
	}, ezcron.Every(2*time.Second))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.Start(ctx)
	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Shutdown(shutdownCtx)

	fmt.Println("scheduler stopped")
}
```

```
$ go run main.go
12:00:02 — hello from ez-cron!
12:00:04 — hello from ez-cron!
12:00:06 — hello from ez-cron!
^C
scheduler stopped
```

That's it. Three calls — `New`, `Add`, `Start` — and you have a running cron system.

---

## 3. Core Concepts

### JobFunc

```go
type JobFunc func(ctx context.Context) error
```

Every job is a plain function. No interfaces to implement, no structs to embed. The `ctx` it receives is cancelled when the scheduler stops or when the job's per-execution timeout elapses. Return a non-nil error to record a failure in metrics.

```go
// A closure works.
s.Add("inline", func(ctx context.Context) error {
    fmt.Println("tick")
    return nil
}, ezcron.Every(time.Second))

// A named function works.
func cleanup(ctx context.Context) error {
    return os.RemoveAll("/tmp/cache")
}
s.Add("cleanup", cleanup, ezcron.Every(ezcron.Daily))

// A method works — just bind it.
type Mailer struct{ smtp string }
func (m *Mailer) SendDigest(ctx context.Context) error { /* ... */ return nil }

mailer := &Mailer{smtp: "mail.example.com"}
s.Add("digest", mailer.SendDigest, ezcron.Every(ezcron.Daily))
```

### Schedule

```go
type Schedule interface {
    Next(now time.Time) time.Time
}
```

A `Schedule` answers one question: *given the current time, when should this job fire next?* The library ships two implementations — `Every` and `Cron` — but you can implement your own for any pattern.

### Scheduler

```go
type Scheduler struct { /* unexported fields */ }
```

The `Scheduler` is the central coordinator. It holds a registry of jobs, launches a goroutine per job on `Start`, and manages the full lifecycle: add, remove, pause, resume, stop, shutdown, and metrics. All methods are safe for concurrent use.

---

## 4. Scheduling

### Fixed Intervals with `Every`

`Every(d)` returns a schedule that fires repeatedly at a fixed duration.

```go
// Fire every 500 milliseconds.
s.Add("fast", myFunc, ezcron.Every(500*time.Millisecond))

// Fire every 30 minutes.
s.Add("report", generateReport, ezcron.Every(30*time.Minute))
```

Durations below 1 millisecond are clamped to 1ms to prevent busy loops.

### Predefined Interval Constants

For common durations you can skip `time.Duration` arithmetic entirely:

```go
ezcron.EverySecond      // 1s
ezcron.Every5Seconds    // 5s
ezcron.Every30Seconds   // 30s
ezcron.EveryMinute      // 1m
ezcron.Every5Minutes    // 5m
ezcron.Every15Minutes   // 15m
ezcron.Every30Minutes   // 30m
ezcron.EveryHour        // 1h
ezcron.Daily            // 24h
ezcron.Weekly           // 168h
```

```go
s.Add("heartbeat", ping, ezcron.Every(ezcron.Every5Seconds))
s.Add("backup", backupDB, ezcron.Every(ezcron.Daily))
```

### Cron Expressions with `Cron`

`Cron(expr)` parses a standard 5-field cron expression and returns a `Schedule`.

```go
// Every weekday at 9:30 AM.
schedule, err := ezcron.Cron("30 9 * * 1-5")
if err != nil {
    log.Fatal(err)
}
s.Add("standup-reminder", remind, schedule)
```

```go
// 1st of every month at midnight.
monthly, _ := ezcron.Cron("0 0 1 * *")
s.Add("invoice", generateInvoice, monthly)

// Every 15 minutes.
quarterly, _ := ezcron.Cron("*/15 * * * *")
s.Add("metrics-push", pushMetrics, quarterly)

// Weekends at 6 AM.
weekend, _ := ezcron.Cron("0 6 * * SAT,SUN")
s.Add("weekend-report", weekendReport, weekend)
```

### Cron Syntax Reference

```
┌───────────── minute       (0 – 59)
│ ┌───────────── hour         (0 – 23)
│ │ ┌───────────── day of month (1 – 31)
│ │ │ ┌───────────── month        (1 – 12 or JAN – DEC)
│ │ │ │ ┌───────────── day of week  (0 – 6 or SUN – SAT, 0 = Sunday)
│ │ │ │ │
* * * * *
```

| Syntax | Meaning | Example |
|--------|---------|---------|
| `*` | Every value | `* * * * *` = every minute |
| `N` | Specific value | `30 * * * *` = at minute 30 |
| `N-M` | Range | `1-5` = Monday through Friday |
| `N,M,O` | List | `0,15,30,45` = four times per hour |
| `*/N` | Every N | `*/10 * * * *` = every 10 minutes |
| `N-M/S` | Range with step | `0-30/10` = at 0, 10, 20, 30 |
| `JAN`-`DEC` | Month names | `* * * JAN,JUL *` = Jan and Jul |
| `SUN`-`SAT` | Day names | `* * * * MON-FRI` = weekdays |

**DOM/DOW interaction**: When both day-of-month and day-of-week are specified (neither is `*`), the job runs if **either** matches (OR logic), following standard cron behavior.

### Cron Shortcuts

| Shortcut | Equivalent | Meaning |
|----------|-----------|---------|
| `@yearly` | `0 0 1 1 *` | Midnight, January 1st |
| `@annually` | `0 0 1 1 *` | Same as @yearly |
| `@monthly` | `0 0 1 * *` | Midnight, 1st of every month |
| `@weekly` | `0 0 * * 0` | Midnight every Sunday |
| `@daily` | `0 0 * * *` | Midnight every day |
| `@midnight` | `0 0 * * *` | Same as @daily |
| `@hourly` | `0 * * * *` | Top of every hour |

```go
daily, _ := ezcron.Cron("@daily")
s.Add("rotate-logs", rotateLogs, daily)
```

### Custom Schedules

Implement the `Schedule` interface to create any scheduling logic:

```go
// BusinessHoursSchedule fires every d during 9 AM – 5 PM on weekdays.
type BusinessHoursSchedule struct {
    interval time.Duration
}

func (b *BusinessHoursSchedule) Next(now time.Time) time.Time {
    next := now.Add(b.interval)
    for {
        if next.Weekday() >= time.Monday &&
            next.Weekday() <= time.Friday &&
            next.Hour() >= 9 &&
            next.Hour() < 17 {
            return next
        }
        // Jump to 9 AM of the next weekday.
        next = time.Date(next.Year(), next.Month(), next.Day()+1,
            9, 0, 0, 0, next.Location())
        for next.Weekday() == time.Saturday || next.Weekday() == time.Sunday {
            next = next.AddDate(0, 0, 1)
        }
    }
}

s.Add("work-check", checkSlack, &BusinessHoursSchedule{interval: 10 * time.Minute})
```

---

## 5. Job Options

Options are passed as trailing arguments to `Add`. They are composable — use as many as you need.

### `WithMaxRuns` — Run N Times Then Stop

The job automatically stops after N executions. Its goroutine exits and state becomes `"stopped"`.

```go
// Send exactly 3 welcome emails, then stop.
s.Add("welcome-drip", sendWelcome, ezcron.Every(ezcron.Daily),
    ezcron.WithMaxRuns(3),
)
```

### `WithRunUntil` — Stop After a Deadline

The job stops after the given wall-clock time passes. Checked after each execution.

```go
// Run a promotional task until the end of the sale.
saleEnd := time.Date(2025, 7, 1, 0, 0, 0, 0, time.Local)
s.Add("sale-banner", refreshBanner, ezcron.Every(ezcron.EveryHour),
    ezcron.WithRunUntil(saleEnd),
)
```

### `WithStopWhen` — Stop on a Condition

Supply a predicate function; the job stops when it returns true. Checked after each execution.

```go
var processed int64

s.Add("batch-import", func(ctx context.Context) error {
    n, err := importNextBatch(ctx)
    atomic.AddInt64(&processed, int64(n))
    return err
}, ezcron.Every(5*time.Second),
    ezcron.WithStopWhen(func() bool {
        return atomic.LoadInt64(&processed) >= 10_000
    }),
)
```

### `WithTimeout` — Per-Execution Time Limit

Each invocation of the job gets its own context with a deadline. If the job takes longer than the timeout, `ctx.Done()` fires.

```go
// Each run of this job must complete within 30 seconds.
s.Add("external-sync", syncFromAPI, ezcron.Every(ezcron.Every5Minutes),
    ezcron.WithTimeout(30*time.Second),
)

// Inside the job, respect the context.
func syncFromAPI(ctx context.Context) error {
    req, _ := http.NewRequestWithContext(ctx, "GET", "https://api.example.com/data", nil)
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err // context.DeadlineExceeded if timeout hit
    }
    defer resp.Body.Close()
    // process response...
    return nil
}
```

### `WithTags` — Tag Jobs for Grouping

Attach string tags for categorization. Tags appear in `JobInfo` when you call `Jobs()`.

```go
s.Add("db-vacuum", vacuumDB, ezcron.Every(ezcron.Daily),
    ezcron.WithTags("database", "maintenance"),
)
s.Add("db-backup", backupDB, ezcron.Every(ezcron.Daily),
    ezcron.WithTags("database", "backup"),
)

// Later, filter by tag.
for _, job := range s.Jobs() {
    for _, tag := range job.Tags {
        if tag == "database" {
            fmt.Printf("DB job: %s (%s)\n", job.Name, job.State)
        }
    }
}
```

### `WithMiddleware` — Attach Middleware

Wrap the job function with one or more middleware. See the [Middleware](#6-middleware) chapter for details.

```go
s.Add("fragile-job", fragileTask, ezcron.Every(ezcron.EveryMinute),
    ezcron.WithMiddleware(
        ezcron.Recover(),
        ezcron.Retry(3),
    ),
)
```

### Combining Options

All options compose freely:

```go
s.Add("critical-sync", syncData, ezcron.Every(ezcron.Every5Minutes),
    ezcron.WithTimeout(2*time.Minute),
    ezcron.WithMaxRuns(100),
    ezcron.WithTags("sync", "critical"),
    ezcron.WithMiddleware(
        ezcron.Recover(),
        ezcron.Retry(3),
        ezcron.LogErrors(func(err error) {
            slog.Error("critical-sync failed", "err", err)
        }),
    ),
)
```

---

## 6. Middleware

```go
type Middleware func(JobFunc) JobFunc
```

Middleware wraps a job function to add cross-cutting behavior. The first middleware in the list wraps outermost, so it sees the final error and runs first/last.

### `Recover` — Catch Panics

Converts panics into errors so one crashing job doesn't bring down the scheduler.

```go
s.Add("risky", func(ctx context.Context) error {
    // If this panics, Recover catches it and returns an error.
    result := riskyComputation()
    return saveResult(ctx, result)
}, ezcron.Every(ezcron.EveryMinute),
    ezcron.WithMiddleware(ezcron.Recover()),
)
```

Without `Recover`, a panic in any job goroutine would crash the entire process. With it, the panic is captured as an `error` with the message `"panic: <value>"` and recorded in metrics as a failure.

### `Retry` — Automatic Retries

Re-executes the job up to N times on failure, with a linear backoff (100ms, 200ms, 300ms...). Stops early if the context is cancelled.

```go
// Try up to 3 times before giving up.
s.Add("webhook", sendWebhook, ezcron.Every(ezcron.EveryMinute),
    ezcron.WithMiddleware(ezcron.Retry(3)),
)
```

From the scheduler's perspective, `Retry` collapses multiple attempts into one execution. If attempt 3 succeeds, the job records a success. If all 3 fail, it records one failure with the last error.

### `LogErrors` — Error Reporting

Calls your function whenever the job returns a non-nil error. The error still propagates to metrics — `LogErrors` is an observer, not a swallower.

```go
s.Add("ingest", ingestData, ezcron.Every(ezcron.Every5Seconds),
    ezcron.WithMiddleware(
        ezcron.LogErrors(func(err error) {
            slog.Error("ingest failed", "error", err)
            alerting.Notify("ingest-failure", err.Error())
        }),
    ),
)
```

### Writing Custom Middleware

Any `func(JobFunc) JobFunc` is valid middleware. Here are practical examples:

**Timing middleware** — log how long each execution takes:

```go
func Timing(log *slog.Logger) ezcron.Middleware {
    return func(next ezcron.JobFunc) ezcron.JobFunc {
        return func(ctx context.Context) error {
            start := time.Now()
            err := next(ctx)
            log.Info("job executed", "duration", time.Since(start), "error", err)
            return err
        }
    }
}

s.Add("task", myTask, ezcron.Every(ezcron.EveryMinute),
    ezcron.WithMiddleware(Timing(slog.Default())),
)
```

**Distributed lock middleware** — skip execution if another instance holds the lock:

```go
func WithLock(redis *redis.Client, key string, ttl time.Duration) ezcron.Middleware {
    return func(next ezcron.JobFunc) ezcron.JobFunc {
        return func(ctx context.Context) error {
            ok, err := redis.SetNX(ctx, key, "locked", ttl).Result()
            if err != nil || !ok {
                return nil // another instance is handling it
            }
            defer redis.Del(ctx, key)
            return next(ctx)
        }
    }
}

s.Add("leader-task", leaderWork, ezcron.Every(ezcron.EveryMinute),
    ezcron.WithMiddleware(WithLock(rdb, "lock:leader-task", 55*time.Second)),
)
```

**Rate limiter middleware** — skip if called too frequently:

```go
func RateLimit(minInterval time.Duration) ezcron.Middleware {
    var lastRun int64
    return func(next ezcron.JobFunc) ezcron.JobFunc {
        return func(ctx context.Context) error {
            now := time.Now().UnixNano()
            last := atomic.LoadInt64(&lastRun)
            if time.Duration(now-last) < minInterval {
                return nil // skip this tick
            }
            atomic.StoreInt64(&lastRun, now)
            return next(ctx)
        }
    }
}
```

### Stacking Middleware

Middleware is applied outermost-first. In this example, the execution order is:
`Recover` -> `Retry` -> `LogErrors` -> `your function`.

```go
s.Add("pipeline", processQueue, ezcron.Every(ezcron.Every5Seconds),
    ezcron.WithMiddleware(
        ezcron.Recover(),               // 1. catches panics from everything below
        ezcron.Retry(3),                // 2. retries on failure (including recovered panics)
        ezcron.LogErrors(func(err error) { // 3. logs errors that survive retries
            slog.Error("pipeline failed after retries", "err", err)
        }),
    ),
)
```

The outermost middleware (`Recover`) runs first on entry and last on exit. If the inner function panics, `Recover` catches it. If it returns an error, `Retry` will re-attempt. If all retries fail, `LogErrors` logs the final error.

---

## 7. Job Management

### Adding Jobs Before Start

The most common pattern. Register everything, then start.

```go
s := ezcron.New()

s.Add("job-a", taskA, ezcron.Every(ezcron.EverySecond))
s.Add("job-b", taskB, ezcron.Every(ezcron.Every5Minutes))
s.Add("job-c", taskC, ezcron.Every(ezcron.Daily))

s.Start(ctx) // all three goroutines launch here
```

### Adding Jobs at Runtime

Jobs added after `Start` are launched immediately.

```go
s := ezcron.New()
s.Start(ctx)

// Later, perhaps in an HTTP handler:
http.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
    name := r.FormValue("name")
    err := s.Add(name, dynamicTask, ezcron.Every(ezcron.EveryMinute))
    if err != nil {
        http.Error(w, err.Error(), 400)
        return
    }
    w.WriteHeader(201)
})
```

### Removing Jobs

`Remove` cancels the job's goroutine, waits for it to exit, and deletes it from the registry. Safe to call while the scheduler is running.

```go
err := s.Remove("job-a")
if errors.Is(err, ezcron.ErrJobNotFound) {
    // already removed or never existed
}
```

### Pausing and Resuming Jobs

`PauseJob` stops a job's goroutine (cancelling any in-progress sleep or execution) and marks it as paused. `ResumeJob` launches a fresh goroutine.

```go
// Pause during a maintenance window.
s.PauseJob("db-writes")

// ... perform maintenance ...

// Resume.
s.ResumeJob("db-writes")
```

State transitions:
```
Add          -> idle
Start        -> running
PauseJob     -> paused      (goroutine exits)
ResumeJob    -> running     (new goroutine launches)
Stop/Remove  -> stopped
MaxRuns hit  -> stopped     (automatic)
```

### Listing All Jobs

`Jobs()` returns a snapshot of every registered job in insertion order.

```go
for _, job := range s.Jobs() {
    fmt.Printf("%-20s state=%-8s runs=%d failures=%d\n",
        job.Name, job.State, job.Metrics.TotalRuns, job.Metrics.Failures)
}
```

```
cleanup              state=running  runs=42  failures=0
sync-api             state=running  runs=18  failures=3
welcome-drip         state=stopped  runs=3   failures=0
db-writes            state=paused   runs=100 failures=1
```

---

## 8. Metrics and Monitoring

### Reading Job Metrics

Every execution is tracked automatically. `JobMetrics` returns a point-in-time snapshot.

```go
type Metrics struct {
    TotalRuns     int64
    Successes     int64
    Failures      int64
    LastRunAt     time.Time
    LastError     error
    TotalDuration time.Duration
    AvgDuration   time.Duration
}
```

```go
m, err := s.JobMetrics("sync-api")
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d | OK: %d | Fail: %d | Avg: %v\n",
    m.TotalRuns, m.Successes, m.Failures, m.AvgDuration)

if m.LastError != nil {
    fmt.Printf("Last error: %v (at %v)\n", m.LastError, m.LastRunAt)
}
```

### Building a Health Endpoint

Expose metrics over HTTP for monitoring dashboards:

```go
http.HandleFunc("GET /health/jobs", func(w http.ResponseWriter, r *http.Request) {
    jobs := s.Jobs()
    w.Header().Set("Content-Type", "application/json")

    type jobStatus struct {
        Name      string `json:"name"`
        State     string `json:"state"`
        Runs      int64  `json:"total_runs"`
        Failures  int64  `json:"failures"`
        AvgMs     int64  `json:"avg_duration_ms"`
        LastError string `json:"last_error,omitempty"`
    }

    var out []jobStatus
    for _, j := range jobs {
        js := jobStatus{
            Name:     j.Name,
            State:    j.State,
            Runs:     j.Metrics.TotalRuns,
            Failures: j.Metrics.Failures,
            AvgMs:    j.Metrics.AvgDuration.Milliseconds(),
        }
        if j.Metrics.LastError != nil {
            js.LastError = j.Metrics.LastError.Error()
        }
        out = append(out, js)
    }
    json.NewEncoder(w).Encode(out)
})
```

---

## 9. Shutdown Patterns

### Immediate Stop

`Stop` cancels every job context and blocks until all goroutines have exited. Jobs that are mid-execution receive a cancelled context.

```go
s.Stop()
// All goroutines are guaranteed to have exited when Stop returns.
```

### Graceful Shutdown

`Shutdown(ctx)` tells all goroutines to stop scheduling new runs, then waits for any in-flight executions to finish. If the provided context expires first, it hard-cancels everything.

```go
// Give jobs up to 30 seconds to finish their current work.
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := s.Shutdown(ctx); err != nil {
    log.Printf("shutdown timed out: %v", err)
}
```

**How it works internally**:

1. Closes an internal `stopCh` channel — sleeping goroutines wake up and exit immediately.
2. Goroutines currently executing a job function keep running (their context is still alive).
3. Once all goroutines finish, `Shutdown` returns `nil`.
4. If the deadline expires, it hard-cancels all contexts, waits for goroutines, and returns `ctx.Err()`.

### Signal-Based Shutdown

The production pattern: run until the OS tells you to stop, then shut down gracefully.

```go
func main() {
    s := ezcron.New()
    // ... add jobs ...

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    s.Start(ctx)

    // Block until signal.
    <-ctx.Done()
    log.Println("shutting down...")

    shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    if err := s.Shutdown(shutdownCtx); err != nil {
        log.Printf("forced shutdown: %v", err)
    }
    log.Println("clean exit")
}
```

---

## 10. Error Handling

The library uses four sentinel errors. Compare with `errors.Is`:

```go
var (
    ErrJobExists     // Add: a job with this name is already registered
    ErrJobNotFound   // Remove/Pause/Resume/JobMetrics: no job with this name
    ErrJobNotRunning // PauseJob: the job is not in the "running" state
    ErrJobNotPaused  // ResumeJob: the job is not in the "paused" state
)
```

```go
err := s.Add("sync", syncFunc, ezcron.Every(time.Minute))

switch {
case errors.Is(err, ezcron.ErrJobExists):
    log.Println("job already registered, skipping")
case err != nil:
    log.Fatalf("unexpected error: %v", err)
}
```

`Add` also returns plain errors for invalid arguments:

```go
s.Add("", fn, sched)    // "ezcron: job name must not be empty"
s.Add("x", nil, sched)  // "ezcron: job function must not be nil"
s.Add("x", fn, nil)     // "ezcron: schedule must not be nil"
```

`Cron` returns a descriptive parse error on invalid expressions:

```go
_, err := ezcron.Cron("60 * * * *")
// "ezcron: invalid minute field: value out of range: 60-60 (allowed 0-59)"
```

---

## 11. Full Example Application

A complete application that demonstrates every major feature together: an HTTP API server with background jobs, metrics endpoint, live job management, and graceful shutdown.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	ezcron "github.com/falcon-codz/ez-cron"
)

func main() {
	log := slog.Default()
	s := ezcron.New()

	// ── Job 1: Heartbeat every 10 seconds ──────────────────────────
	s.Add("heartbeat", func(ctx context.Context) error {
		log.Info("heartbeat", "time", time.Now().Format(time.TimeOnly))
		return nil
	}, ezcron.Every(10*time.Second),
		ezcron.WithTags("infra", "monitoring"),
	)

	// ── Job 2: DB cleanup every night at 2 AM ──────────────────────
	nightly, _ := ezcron.Cron("0 2 * * *")
	s.Add("db-cleanup", func(ctx context.Context) error {
		log.Info("running database cleanup")
		time.Sleep(2 * time.Second) // simulate work
		return nil
	}, nightly,
		ezcron.WithTimeout(5*time.Minute),
		ezcron.WithTags("database", "maintenance"),
		ezcron.WithMiddleware(
			ezcron.Recover(),
			ezcron.LogErrors(func(err error) {
				log.Error("db-cleanup failed", "err", err)
			}),
		),
	)

	// ── Job 3: Flaky external sync with retries ────────────────────
	s.Add("api-sync", func(ctx context.Context) error {
		if rand.Intn(3) == 0 {
			return fmt.Errorf("upstream timeout")
		}
		log.Info("api-sync succeeded")
		return nil
	}, ezcron.Every(ezcron.Every30Seconds),
		ezcron.WithTimeout(10*time.Second),
		ezcron.WithTags("sync", "external"),
		ezcron.WithMiddleware(
			ezcron.Recover(),
			ezcron.Retry(3),
			ezcron.LogErrors(func(err error) {
				log.Error("api-sync failed after retries", "err", err)
			}),
		),
	)

	// ── Job 4: One-shot migration (max 1 run) ─────────────────────
	s.Add("run-migration", func(ctx context.Context) error {
		log.Info("migration complete")
		return nil
	}, ezcron.Every(ezcron.EverySecond),
		ezcron.WithMaxRuns(1),
		ezcron.WithTags("database", "migration"),
	)

	// ── HTTP endpoints for management ──────────────────────────────
	mux := http.NewServeMux()

	// GET /jobs — list all jobs with metrics.
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(s.Jobs())
	})

	// POST /jobs/{name}/pause
	mux.HandleFunc("POST /jobs/{name}/pause", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := s.PauseJob(name); err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		fmt.Fprintf(w, "paused %s\n", name)
	})

	// POST /jobs/{name}/resume
	mux.HandleFunc("POST /jobs/{name}/resume", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := s.ResumeJob(name); err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		fmt.Fprintf(w, "resumed %s\n", name)
	})

	// DELETE /jobs/{name}
	mux.HandleFunc("DELETE /jobs/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if err := s.Remove(name); err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		fmt.Fprintf(w, "removed %s\n", name)
	})

	// ── Start everything ───────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.Start(ctx)
	log.Info("scheduler started", "jobs", len(s.Jobs()))

	server := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Info("http server listening", "addr", ":8080")
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
		}
	}()

	// ── Wait for signal, then shut down ────────────────────────────
	<-ctx.Done()
	log.Info("received shutdown signal")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shut down HTTP server and scheduler in parallel.
	server.Shutdown(shutdownCtx)
	if err := s.Shutdown(shutdownCtx); err != nil {
		log.Error("scheduler shutdown", "err", err)
	}
	log.Info("clean shutdown complete")
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, ezcron.ErrJobNotFound):
		return 404
	case errors.Is(err, ezcron.ErrJobNotRunning),
		errors.Is(err, ezcron.ErrJobNotPaused),
		errors.Is(err, ezcron.ErrJobExists):
		return 409
	default:
		return 500
	}
}
```

---

## 12. API Reference

### Constructors

| Function | Signature | Description |
|----------|-----------|-------------|
| `New` | `func New() *Scheduler` | Create a new scheduler |
| `Every` | `func Every(d time.Duration) Schedule` | Fixed-interval schedule |
| `Cron` | `func Cron(expr string) (Schedule, error)` | Cron expression schedule |

### Scheduler Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `Add` | `(name string, fn JobFunc, schedule Schedule, opts ...JobOption) error` | Register a job |
| `Remove` | `(name string) error` | Unregister and stop a job |
| `Start` | `(ctx context.Context)` | Launch all job goroutines |
| `Stop` | `()` | Hard-cancel all jobs, block until exit |
| `Shutdown` | `(ctx context.Context) error` | Graceful stop with deadline |
| `PauseJob` | `(name string) error` | Pause a running job |
| `ResumeJob` | `(name string) error` | Resume a paused job |
| `Jobs` | `() []JobInfo` | List all jobs with state and metrics |
| `JobMetrics` | `(name string) (Metrics, error)` | Get metrics for one job |

### Job Options

| Function | Description |
|----------|-------------|
| `WithTimeout(d)` | Per-execution time limit |
| `WithMaxRuns(n)` | Auto-stop after n runs |
| `WithRunUntil(t)` | Auto-stop after wall-clock deadline |
| `WithStopWhen(fn)` | Auto-stop when predicate returns true |
| `WithTags(tags...)` | Attach metadata tags |
| `WithMiddleware(mw...)` | Wrap with middleware |

### Built-in Middleware

| Function | Description |
|----------|-------------|
| `Recover()` | Convert panics to errors |
| `Retry(n)` | Retry up to n times with linear backoff |
| `LogErrors(fn)` | Call fn on every non-nil error |

### Types

```go
type JobFunc    func(ctx context.Context) error
type Middleware  func(JobFunc) JobFunc
type JobOption   func(*jobOptions)       // opaque; use With* constructors

type Schedule interface {
    Next(now time.Time) time.Time
}

type Metrics struct {
    TotalRuns     int64
    Successes     int64
    Failures      int64
    LastRunAt     time.Time
    LastError     error
    TotalDuration time.Duration
    AvgDuration   time.Duration
}

type JobInfo struct {
    Name    string
    Tags    []string
    State   string          // "idle", "running", "paused", "stopped"
    Metrics Metrics
}
```

### Sentinel Errors

```go
var ErrJobExists     = errors.New("ezcron: job already exists")
var ErrJobNotFound   = errors.New("ezcron: job not found")
var ErrJobNotRunning = errors.New("ezcron: job is not running")
var ErrJobNotPaused  = errors.New("ezcron: job is not paused")
```

### Predefined Intervals

```go
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
```
