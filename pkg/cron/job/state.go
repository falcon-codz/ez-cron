package job

type State int

const (
	Idle State = iota
	Running
	Paused
	Stopped
	Completed
)

// String returns the human-readable name of a job state.
func (s State) String() string {
	switch s {
	case Idle:
		return "idle"
	case Running:
		return "running"
	case Paused:
		return "paused"
	case Stopped:
		return "stopped"
	case Completed:
		return "completed"
	default:
		return "unknown"
	}
}
