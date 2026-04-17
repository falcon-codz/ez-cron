package ezcron

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Schedule: Every
// ---------------------------------------------------------------------------

func TestEvery(t *testing.T) {
	s := Every(2 * time.Second)
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next := s.Next(now)
	if !next.Equal(now.Add(2 * time.Second)) {
		t.Fatalf("expected %v, got %v", now.Add(2*time.Second), next)
	}
}

func TestEveryMinFloor(t *testing.T) {
	s := Every(0)
	now := time.Now()
	next := s.Next(now)
	if d := next.Sub(now); d < time.Millisecond {
		t.Fatalf("expected at least 1ms, got %v", d)
	}
}

// ---------------------------------------------------------------------------
// Schedule: Cron parsing
// ---------------------------------------------------------------------------

func TestCronParseErrors(t *testing.T) {
	bad := []string{
		"",
		"* * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * 32 * *",
		"* * * 13 *",
		"* * * * 8",
		"* * * * abc",
		"*/0 * * * *",
	}
	for _, expr := range bad {
		if _, err := Cron(expr); err == nil {
			t.Errorf("expected error for %q", expr)
		}
	}
}

func TestCronParseValid(t *testing.T) {
	good := []string{
		"* * * * *",
		"0 0 1 1 *",
		"*/5 * * * *",
		"0 2 1-15 * MON-FRI",
		"0 0 * * 0,6",
		"@yearly",
		"@daily",
		"@hourly",
	}
	for _, expr := range good {
		if _, err := Cron(expr); err != nil {
			t.Errorf("unexpected error for %q: %v", expr, err)
		}
	}
}

// ---------------------------------------------------------------------------
// Cron: Next()
// ---------------------------------------------------------------------------

func TestCronNextEveryMinute(t *testing.T) {
	s, _ := Cron("* * * * *")
	now := time.Date(2024, 1, 1, 12, 30, 15, 0, time.UTC)
	got := s.Next(now)
	want := time.Date(2024, 1, 1, 12, 31, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCronNextSpecificTime(t *testing.T) {
	s, _ := Cron("30 2 * * *") // 02:30 daily
	now := time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC)
	got := s.Next(now)
	want := time.Date(2024, 1, 2, 2, 30, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCronNextMonthRollover(t *testing.T) {
	s, _ := Cron("0 0 1 3 *") // midnight on March 1
	now := time.Date(2024, 3, 2, 0, 0, 0, 0, time.UTC)
	got := s.Next(now)
	want := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCronNextDOW(t *testing.T) {
	s, _ := Cron("0 9 * * MON") // Mondays at 09:00
	// 2024-01-01 is a Monday.
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	got := s.Next(now)
	want := time.Date(2024, 1, 8, 9, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCronNextStep(t *testing.T) {
	s, _ := Cron("*/15 * * * *") // every 15 min
	now := time.Date(2024, 1, 1, 0, 14, 0, 0, time.UTC)
	got := s.Next(now)
	want := time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestCronShortcuts(t *testing.T) {
	tests := []struct {
		expr string
		want time.Time
	}{
		{"@yearly", time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"@monthly", time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"@daily", time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)},
		{"@hourly", time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC)},
	}
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tt := range tests {
		s, err := Cron(tt.expr)
		if err != nil {
			t.Fatalf("%s: %v", tt.expr, err)
		}
		got := s.Next(now)
		if !got.Equal(tt.want) {
			t.Errorf("%s: got %v, want %v", tt.expr, got, tt.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Scheduler: basic lifecycle
// ---------------------------------------------------------------------------

func TestSchedulerStartStop(t *testing.T) {
	var count int64
	s := New()
	s.Add("tick", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	s.Stop()

	got := atomic.LoadInt64(&count)
	if got < 2 {
		t.Fatalf("expected at least 2 runs, got %d", got)
	}
}

func TestSchedulerShutdownGraceful(t *testing.T) {
	var count int64
	s := New()
	s.Add("tick", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := s.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown error: %v", err)
	}

	got := atomic.LoadInt64(&count)
	if got < 2 {
		t.Fatalf("expected at least 2 runs, got %d", got)
	}
}

func TestSchedulerContextCancel(t *testing.T) {
	var count int64
	s := New()
	s.Add("tick", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()
	// Give goroutines a moment to exit.
	time.Sleep(50 * time.Millisecond)

	got := atomic.LoadInt64(&count)
	if got < 1 {
		t.Fatalf("expected at least 1 run, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Add / Remove
// ---------------------------------------------------------------------------

func TestAddDuplicate(t *testing.T) {
	s := New()
	s.Add("a", func(ctx context.Context) error { return nil }, Every(time.Second))
	if err := s.Add("a", func(ctx context.Context) error { return nil }, Every(time.Second)); !errors.Is(err, ErrJobExists) {
		t.Fatalf("expected ErrJobExists, got %v", err)
	}
}

func TestAddValidation(t *testing.T) {
	s := New()
	if err := s.Add("", func(ctx context.Context) error { return nil }, Every(time.Second)); err == nil {
		t.Fatal("expected error for empty name")
	}
	if err := s.Add("x", nil, Every(time.Second)); err == nil {
		t.Fatal("expected error for nil fn")
	}
	if err := s.Add("x", func(ctx context.Context) error { return nil }, nil); err == nil {
		t.Fatal("expected error for nil schedule")
	}
}

func TestRemoveNotFound(t *testing.T) {
	s := New()
	if err := s.Remove("ghost"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestRemoveWhileRunning(t *testing.T) {
	var count int64
	s := New()
	s.Add("a", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(200 * time.Millisecond)
	if err := s.Remove("a"); err != nil {
		t.Fatalf("remove error: %v", err)
	}

	countBefore := atomic.LoadInt64(&count)
	time.Sleep(200 * time.Millisecond)
	countAfter := atomic.LoadInt64(&count)

	if countAfter != countBefore {
		t.Fatalf("job kept running after removal: before=%d after=%d", countBefore, countAfter)
	}
	s.Stop()
}

func TestAddWhileRunning(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	var count int64
	s.Add("late", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	time.Sleep(300 * time.Millisecond)
	s.Stop()

	got := atomic.LoadInt64(&count)
	if got < 2 {
		t.Fatalf("expected at least 2 runs for late-added job, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Pause / Resume
// ---------------------------------------------------------------------------

func TestPauseResume(t *testing.T) {
	var count int64
	s := New()
	s.Add("a", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(200 * time.Millisecond)
	if err := s.PauseJob("a"); err != nil {
		t.Fatalf("pause error: %v", err)
	}

	paused := atomic.LoadInt64(&count)
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt64(&count); got != paused {
		t.Fatalf("job ran while paused: paused=%d now=%d", paused, got)
	}

	if err := s.ResumeJob("a"); err != nil {
		t.Fatalf("resume error: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	s.Stop()

	if got := atomic.LoadInt64(&count); got <= paused {
		t.Fatalf("job did not resume: paused=%d final=%d", paused, got)
	}
}

func TestPauseErrors(t *testing.T) {
	s := New()
	s.Add("a", func(ctx context.Context) error { return nil }, Every(time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()

	if err := s.PauseJob("ghost"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
	s.PauseJob("a")
	if err := s.PauseJob("a"); !errors.Is(err, ErrJobNotRunning) {
		t.Fatalf("expected ErrJobNotRunning, got %v", err)
	}
}

func TestResumeErrors(t *testing.T) {
	s := New()
	s.Add("a", func(ctx context.Context) error { return nil }, Every(time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	defer s.Stop()

	if err := s.ResumeJob("ghost"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
	if err := s.ResumeJob("a"); !errors.Is(err, ErrJobNotPaused) {
		t.Fatalf("expected ErrJobNotPaused, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// MaxRuns
// ---------------------------------------------------------------------------

func TestMaxRuns(t *testing.T) {
	var count int64
	s := New()
	s.Add("a", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond), WithMaxRuns(3))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	s.Stop()

	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("expected exactly 3 runs, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// WithStopWhen
// ---------------------------------------------------------------------------

func TestStopWhen(t *testing.T) {
	var count int64
	s := New()
	s.Add("a", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond), WithStopWhen(func() bool {
		return atomic.LoadInt64(&count) >= 3
	}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	s.Stop()

	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("expected 3 runs, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Per-job timeout
// ---------------------------------------------------------------------------

func TestJobTimeout(t *testing.T) {
	var timedOut int64
	s := New()
	s.Add("slow", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			atomic.StoreInt64(&timedOut, 1)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}, Every(50*time.Millisecond), WithTimeout(100*time.Millisecond), WithMaxRuns(1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(500 * time.Millisecond)
	s.Stop()

	if atomic.LoadInt64(&timedOut) != 1 {
		t.Fatal("job did not observe timeout")
	}
}

// ---------------------------------------------------------------------------
// Middleware
// ---------------------------------------------------------------------------

func TestRecoverMiddleware(t *testing.T) {
	var count int64
	s := New()
	s.Add("panic", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		panic("boom")
	}, Every(50*time.Millisecond), WithMiddleware(Recover()), WithMaxRuns(3))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	s.Stop()

	if got := atomic.LoadInt64(&count); got != 3 {
		t.Fatalf("expected 3 runs with recover, got %d", got)
	}

	m, _ := s.JobMetrics("panic")
	if m.Failures != 3 {
		t.Fatalf("expected 3 failures, got %d", m.Failures)
	}
}

func TestRetryMiddleware(t *testing.T) {
	var attempts int64
	s := New()
	s.Add("flaky", func(ctx context.Context) error {
		n := atomic.AddInt64(&attempts, 1)
		if n <= 2 {
			return errors.New("fail")
		}
		return nil
	}, Every(time.Second), WithMiddleware(Retry(3)), WithMaxRuns(1))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(2 * time.Second)
	s.Stop()

	m, _ := s.JobMetrics("flaky")
	if m.Successes != 1 {
		t.Fatalf("expected 1 success, got %d", m.Successes)
	}
	if atomic.LoadInt64(&attempts) != 3 {
		t.Fatalf("expected 3 attempts, got %d", atomic.LoadInt64(&attempts))
	}
}

func TestLogErrorsMiddleware(t *testing.T) {
	var logged int64
	s := New()
	s.Add("err", func(ctx context.Context) error {
		return errors.New("oops")
	}, Every(50*time.Millisecond),
		WithMiddleware(LogErrors(func(err error) {
			atomic.AddInt64(&logged, 1)
		})),
		WithMaxRuns(2),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(300 * time.Millisecond)
	s.Stop()

	if got := atomic.LoadInt64(&logged); got != 2 {
		t.Fatalf("expected 2 logged errors, got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

func TestJobMetrics(t *testing.T) {
	s := New()
	callNum := int64(0)
	s.Add("m", func(ctx context.Context) error {
		n := atomic.AddInt64(&callNum, 1)
		if n%2 == 0 {
			return errors.New("even fail")
		}
		return nil
	}, Every(50*time.Millisecond), WithMaxRuns(4))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	s.Stop()

	m, err := s.JobMetrics("m")
	if err != nil {
		t.Fatal(err)
	}
	if m.TotalRuns != 4 {
		t.Fatalf("total runs: got %d, want 4", m.TotalRuns)
	}
	if m.Successes != 2 {
		t.Fatalf("successes: got %d, want 2", m.Successes)
	}
	if m.Failures != 2 {
		t.Fatalf("failures: got %d, want 2", m.Failures)
	}
	if m.AvgDuration <= 0 {
		t.Fatal("avg duration should be positive")
	}
}

func TestJobMetricsNotFound(t *testing.T) {
	s := New()
	if _, err := s.JobMetrics("nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Jobs listing
// ---------------------------------------------------------------------------

func TestJobsListing(t *testing.T) {
	s := New()
	s.Add("alpha", func(ctx context.Context) error { return nil }, Every(time.Second), WithTags("a", "b"))
	s.Add("beta", func(ctx context.Context) error { return nil }, Every(time.Second))

	jobs := s.Jobs()
	if len(jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(jobs))
	}
	if jobs[0].Name != "alpha" || jobs[1].Name != "beta" {
		t.Fatalf("unexpected order: %v %v", jobs[0].Name, jobs[1].Name)
	}
	if len(jobs[0].Tags) != 2 || jobs[0].Tags[0] != "a" {
		t.Fatalf("unexpected tags: %v", jobs[0].Tags)
	}
}

// ---------------------------------------------------------------------------
// WithRunUntil
// ---------------------------------------------------------------------------

func TestRunUntil(t *testing.T) {
	var count int64
	deadline := time.Now().Add(200 * time.Millisecond)
	s := New()
	s.Add("a", func(ctx context.Context) error {
		atomic.AddInt64(&count, 1)
		return nil
	}, Every(50*time.Millisecond), WithRunUntil(deadline))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(500 * time.Millisecond)
	s.Stop()

	got := atomic.LoadInt64(&count)
	if got < 1 {
		t.Fatal("expected at least 1 run")
	}
	// After 200ms at 50ms intervals we'd get ~4 runs. Verify it stopped.
	if got > 6 {
		t.Fatalf("job should have stopped near deadline, got %d runs", got)
	}
}

// ---------------------------------------------------------------------------
// Concurrent safety
// ---------------------------------------------------------------------------

func TestConcurrentAddRemove(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			name := "j" + time.Now().String() + string(rune(i))
			s.Add(name, func(ctx context.Context) error { return nil }, Every(time.Second))
			s.Remove(name)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent add/remove timed out")
	}
	s.Stop()
}
