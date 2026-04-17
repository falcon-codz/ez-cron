package ezcron

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type cronSchedule struct {
	minutes bitSet
	hours   bitSet
	doms    bitSet
	months  bitSet
	dows    bitSet
	domStar bool
	dowStar bool
}

type bitSet uint64

func (b *bitSet) set(i int) { *b |= 1 << uint(i) }
func (b bitSet) has(i int) bool { return b&(1<<uint(i)) != 0 }

var shortMonths = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
	"MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
	"SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}

var shortDays = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3,
	"THU": 4, "FRI": 5, "SAT": 6,
}

var shortcuts = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

// Cron parses a standard 5-field cron expression and returns a Schedule.
//
// Fields: minute hour day-of-month month day-of-week
//
// Supports *, ranges (1-5), steps (*/5, 1-10/2), lists (1,3,5),
// month names (JAN-DEC), day names (SUN-SAT), and shortcuts
// (@yearly, @monthly, @weekly, @daily, @hourly).
func Cron(expr string) (Schedule, error) {
	expr = strings.TrimSpace(expr)

	if expanded, ok := shortcuts[strings.ToLower(expr)]; ok {
		expr = expanded
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("ezcron: cron expression must have 5 fields, got %d", len(fields))
	}

	c := &cronSchedule{}
	var err error

	c.minutes, err = parseField(fields[0], 0, 59, nil)
	if err != nil {
		return nil, fmt.Errorf("ezcron: invalid minute field: %w", err)
	}

	c.hours, err = parseField(fields[1], 0, 23, nil)
	if err != nil {
		return nil, fmt.Errorf("ezcron: invalid hour field: %w", err)
	}

	c.doms, err = parseField(fields[2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("ezcron: invalid day-of-month field: %w", err)
	}
	c.domStar = fields[2] == "*"

	c.months, err = parseField(fields[3], 1, 12, shortMonths)
	if err != nil {
		return nil, fmt.Errorf("ezcron: invalid month field: %w", err)
	}

	c.dows, err = parseField(fields[4], 0, 6, shortDays)
	if err != nil {
		return nil, fmt.Errorf("ezcron: invalid day-of-week field: %w", err)
	}
	c.dowStar = fields[4] == "*"

	// 7 is an alias for Sunday in some cron implementations.
	if c.dows.has(7) {
		c.dows.set(0)
	}

	return c, nil
}

func parseField(field string, min, max int, names map[string]int) (bitSet, error) {
	var bs bitSet
	for _, part := range strings.Split(field, ",") {
		if err := parsePart(part, min, max, names, &bs); err != nil {
			return 0, err
		}
	}
	return bs, nil
}

func parsePart(part string, min, max int, names map[string]int, bs *bitSet) error {
	step := 1
	if i := strings.Index(part, "/"); i >= 0 {
		s, err := strconv.Atoi(part[i+1:])
		if err != nil || s <= 0 {
			return fmt.Errorf("invalid step in %q", part)
		}
		step = s
		part = part[:i]
	}

	var lo, hi int
	switch {
	case part == "*":
		lo, hi = min, max
	case strings.Contains(part, "-"):
		i := strings.Index(part, "-")
		var err error
		lo, err = parseValue(part[:i], names)
		if err != nil {
			return err
		}
		hi, err = parseValue(part[i+1:], names)
		if err != nil {
			return err
		}
	default:
		v, err := parseValue(part, names)
		if err != nil {
			return err
		}
		lo, hi = v, v
	}

	if lo < min || hi > max || lo > hi {
		return fmt.Errorf("value out of range: %d-%d (allowed %d-%d)", lo, hi, min, max)
	}

	for i := lo; i <= hi; i += step {
		bs.set(i)
	}
	return nil
}

func parseValue(s string, names map[string]int) (int, error) {
	if names != nil {
		if v, ok := names[strings.ToUpper(s)]; ok {
			return v, nil
		}
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid value: %q", s)
	}
	return v, nil
}

func (c *cronSchedule) matchDay(t time.Time) bool {
	if c.domStar && c.dowStar {
		return true
	}
	if c.domStar {
		return c.dows.has(int(t.Weekday()))
	}
	if c.dowStar {
		return c.doms.has(t.Day())
	}
	// Both specified: standard cron OR logic.
	return c.doms.has(t.Day()) || c.dows.has(int(t.Weekday()))
}

// Next returns the next time after now that matches the cron schedule.
// It searches up to 4 years ahead; if no match is found it returns the zero Time.
func (c *cronSchedule) Next(now time.Time) time.Time {
	// Start from the next whole minute.
	t := now.Add(time.Minute).Truncate(time.Minute)
	limit := t.AddDate(4, 0, 0)

	for t.Before(limit) {
		if !c.months.has(int(t.Month())) {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}

		if !c.matchDay(t) {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}

		if !c.hours.has(t.Hour()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}

		if !c.minutes.has(t.Minute()) {
			t = t.Add(time.Minute)
			continue
		}

		return t
	}

	return time.Time{}
}
