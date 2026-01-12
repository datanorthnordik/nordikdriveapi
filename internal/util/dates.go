package util

import (
	"errors"
	"strings"
	"time"
)

func ParseDateRange(startStr, endStr *string) (start time.Time, hasStart bool, endExclusive time.Time, hasEnd bool, err error) {
	parseAny := func(s string) (t time.Time, ok bool, isDateOnly bool, err error) {
		s = strings.TrimSpace(s)
		if s == "" {
			return time.Time{}, false, false, nil
		}

		// RFC3339 timestamp
		if tt, e := time.Parse(time.RFC3339, s); e == nil {
			return tt, true, false, nil
		}

		// YYYY-MM-DD
		if tt, e := time.Parse("2006-01-02", s); e == nil {
			return tt, true, true, nil // date-only => start of day
		}

		return time.Time{}, false, false, errors.New("invalid date format (use YYYY-MM-DD or RFC3339)")
	}

	var (
		rawStart       time.Time
		rawEnd         time.Time
		startOk        bool
		endOk          bool
		startDateOnly  bool
		endDateOnly    bool
	)

	if startStr != nil {
		t, ok, isDateOnly, e := parseAny(*startStr)
		if e != nil {
			return time.Time{}, false, time.Time{}, false, e
		}
		if ok {
			rawStart = t
			startOk = true
			startDateOnly = isDateOnly
			_ = startDateOnly // not used, but kept if you want later
		}
	}

	if endStr != nil {
		t, ok, isDateOnly, e := parseAny(*endStr)
		if e != nil {
			return time.Time{}, false, time.Time{}, false, e
		}
		if ok {
			rawEnd = t
			endOk = true
			endDateOnly = isDateOnly
		}
	}

	// If reversed, swap RAW values (not endExclusive)
	if startOk && endOk && rawEnd.Before(rawStart) {
		rawStart, rawEnd = rawEnd, rawStart
		// If the "end" came from the start input, it might be date-only now.
		// We can't reliably know which one user intended, so treat end as date-only
		// ONLY if the actual end string was date-only.
		// (So: keep endDateOnly as parsed from endStr)
	}

	// Build outputs
	if startOk {
		start = rawStart
		hasStart = true
	}

	if endOk {
		if endDateOnly {
			endExclusive = rawEnd.AddDate(0, 0, 1) // include the whole end date
		} else {
			endExclusive = rawEnd // exclusive boundary for timestamp
		}
		hasEnd = true
	}

	return start, hasStart, endExclusive, hasEnd, nil
}
