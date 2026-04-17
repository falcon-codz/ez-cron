package ezcron

import "errors"

var (
	ErrJobExists    = errors.New("ezcron: job already exists")
	ErrJobNotFound  = errors.New("ezcron: job not found")
	ErrJobNotRunning = errors.New("ezcron: job is not running")
	ErrJobNotPaused = errors.New("ezcron: job is not paused")
)
