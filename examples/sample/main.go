// Sample program demonstrating ez-cron: scheduling, options, middleware,
// metrics export over HTTP, and graceful shutdown on SIGINT/SIGTERM.
//
// Run:
//   go run ./examples/sample
//
// Then in another terminal:
//   curl http://localhost:8080/metrics | jq
//   curl http://localhost:8080/jobs    | jq
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	ezcron "github.com/falcon-codz/ez-cron"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	s := ezcron.New()

	// 1. Heartbeat — plain interval, tagged for grouping.
	s.Add("heartbeat", func(ctx context.Context) error {
		logger.Info("heartbeat", "at", time.Now().Format(time.TimeOnly))
		return nil
	}, ezcron.Every(5*time.Second),
		ezcron.WithTags("infra", "monitoring"),
	)

	// 2. Flaky API sync — fails randomly to exercise Retry + LogErrors.
	s.Add("api-sync", func(ctx context.Context) error {
		if rand.Intn(3) == 0 {
			return fmt.Errorf("upstream timeout (simulated)")
		}
		return nil
	}, ezcron.Every(7*time.Second),
		ezcron.WithTimeout(3*time.Second),
		ezcron.WithTags("sync", "external"),
		ezcron.WithMiddleware(
			ezcron.Recover(),
			ezcron.Retry(3),
			ezcron.LogErrors(func(err error) {
				logger.Error("api-sync failed after retries", "err", err)
			}),
		),
	)

	// 3. One-shot migration — runs exactly once then stops.
	s.Add("bootstrap-migration", func(ctx context.Context) error {
		logger.Info("migration complete")
		return nil
	}, ezcron.Every(2*time.Second),
		ezcron.WithMaxRuns(1),
		ezcron.WithTags("database", "one-shot"),
	)

	// 4. Cron-expression job — every minute at :30 seconds offset conceptually,
	//    but cron resolution is 1 minute, so this fires at the top of each minute.
	everyMinute, err := ezcron.Cron("* * * * *")
	if err != nil {
		log.Fatalf("cron parse: %v", err)
	}
	s.Add("minute-tick", func(ctx context.Context) error {
		logger.Info("minute tick")
		return nil
	}, everyMinute,
		ezcron.WithTags("cron"),
	)

	// HTTP endpoints for metrics and listing.
	mux := http.NewServeMux()

	mux.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(s.Jobs())
	})

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		type row struct {
			Name          string `json:"name"`
			State         string `json:"state"`
			Tags          []string `json:"tags"`
			TotalRuns     int64  `json:"total_runs"`
			Successes     int64  `json:"successes"`
			Failures      int64  `json:"failures"`
			AvgDurationMs int64  `json:"avg_duration_ms"`
			LastRunAt     string `json:"last_run_at,omitempty"`
			LastError     string `json:"last_error,omitempty"`
		}
		out := []row{}
		for _, j := range s.Jobs() {
			r := row{
				Name:          j.Name,
				State:         j.State,
				Tags:          j.Tags,
				TotalRuns:     j.Metrics.TotalRuns,
				Successes:     j.Metrics.Successes,
				Failures:      j.Metrics.Failures,
				AvgDurationMs: j.Metrics.AvgDuration.Milliseconds(),
			}
			if !j.Metrics.LastRunAt.IsZero() {
				r.LastRunAt = j.Metrics.LastRunAt.Format(time.RFC3339)
			}
			if j.Metrics.LastError != nil {
				r.LastError = j.Metrics.LastError.Error()
			}
			out = append(out, r)
		}
		_ = json.NewEncoder(w).Encode(out)
	})

	// POST /jobs/pause?name=foo and /jobs/resume?name=foo
	mux.HandleFunc("/jobs/pause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := s.PauseJob(r.URL.Query().Get("name")); err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		fmt.Fprintln(w, "paused")
	})

	mux.HandleFunc("/jobs/resume", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := s.ResumeJob(r.URL.Query().Get("name")); err != nil {
			http.Error(w, err.Error(), statusFor(err))
			return
		}
		fmt.Fprintln(w, "resumed")
	})

	// Start scheduler and HTTP server.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s.Start(ctx)
	logger.Info("scheduler started", "jobs", len(s.Jobs()))

	server := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		logger.Info("http server listening", "addr", ":8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", "err", err)
		}
	}()

	// Print a metrics table to stdout every 15s so you can see progress without curl.
	go printMetricsLoop(ctx, s)

	<-ctx.Done()
	logger.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = server.Shutdown(shutdownCtx)
	if err := s.Shutdown(shutdownCtx); err != nil {
		logger.Error("scheduler shutdown", "err", err)
	}
	logger.Info("clean exit")
}

func printMetricsLoop(ctx context.Context, s *ezcron.Scheduler) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Println("\n---- metrics snapshot ----")
			fmt.Printf("%-22s %-8s %-6s %-6s %-6s %-10s\n", "NAME", "STATE", "RUNS", "OK", "FAIL", "AVG")
			for _, j := range s.Jobs() {
				fmt.Printf("%-22s %-8s %-6d %-6d %-6d %-10s\n",
					j.Name, j.State,
					j.Metrics.TotalRuns, j.Metrics.Successes, j.Metrics.Failures,
					j.Metrics.AvgDuration.Round(time.Millisecond),
				)
			}
			fmt.Println("--------------------------")
		}
	}
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, ezcron.ErrJobNotFound):
		return http.StatusNotFound
	case errors.Is(err, ezcron.ErrJobNotRunning),
		errors.Is(err, ezcron.ErrJobNotPaused),
		errors.Is(err, ezcron.ErrJobExists):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}
