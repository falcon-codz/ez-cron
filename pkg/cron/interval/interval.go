package interval

import "time"

const (
	EverySecond   = 1 * time.Second
	Every5Seconds = 5 * time.Second
	Every30Seconds = 30 * time.Second
	EveryMinute    = 1 * time.Minute
	Every5Minutes  = 5 * time.Minute
	Every15Minutes = 15 * time.Minute
	Every30Minutes = 30 * time.Minute
	EveryHour      = 1 * time.Hour
	Daily          = 24 * time.Hour
	Weekly         = 7 * Daily
)
