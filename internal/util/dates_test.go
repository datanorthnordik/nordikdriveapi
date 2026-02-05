package util

import (
	"testing"
	"time"
)

func sptr(s string) *string { return &s }

func mustTimeRFC3339(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse RFC3339 %q: %v", s, err)
	}
	return tt
}

func mustTimeDate(t *testing.T, s string) time.Time {
	t.Helper()
	tt, err := time.Parse("2006-01-02", s)
	if err != nil {
		t.Fatalf("parse date %q: %v", s, err)
	}
	return tt
}

func TestParseDateRange_AllNil(t *testing.T) {
	start, hasStart, endExcl, hasEnd, err := ParseDateRange(nil, nil)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if hasStart || hasEnd {
		t.Fatalf("expected no start/end, got hasStart=%v hasEnd=%v", hasStart, hasEnd)
	}
	if !start.IsZero() || !endExcl.IsZero() {
		t.Fatalf("expected zero times, got start=%v end=%v", start, endExcl)
	}
}

func TestParseDateRange_BlankStrings_TreatedAsMissing(t *testing.T) {
	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr("   "), sptr(""))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if hasStart || hasEnd {
		t.Fatalf("expected no start/end, got hasStart=%v hasEnd=%v", hasStart, hasEnd)
	}
	if !start.IsZero() || !endExcl.IsZero() {
		t.Fatalf("expected zero times, got start=%v end=%v", start, endExcl)
	}
}

func TestParseDateRange_RFC3339Start_DateOnlyEnd_EndExclusiveAddsOneDay(t *testing.T) {
	startStr := "2026-02-03T10:00:00Z"
	endStr := "2026-02-05"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeRFC3339(t, startStr)
	wantEndExcl := mustTimeDate(t, endStr).AddDate(0, 0, 1)

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_DateOnlyStart_DateOnlyEnd(t *testing.T) {
	startStr := "2026-02-03"
	endStr := "2026-02-05"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeDate(t, startStr)
	wantEndExcl := mustTimeDate(t, endStr).AddDate(0, 0, 1)

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_TimestampEnd_IsExclusiveAtSameMoment(t *testing.T) {
	startStr := "2026-02-03T10:00:00Z"
	endStr := "2026-02-03T12:00:00Z"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeRFC3339(t, startStr)
	wantEndExcl := mustTimeRFC3339(t, endStr)

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_InvalidStartFormat_ReturnsError(t *testing.T) {
	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr("02/03/2026"), nil)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if hasStart || hasEnd {
		t.Fatalf("expected hasStart/hasEnd false, got %v %v", hasStart, hasEnd)
	}
	if !start.IsZero() || !endExcl.IsZero() {
		t.Fatalf("expected zero times on error, got start=%v end=%v", start, endExcl)
	}
}

func TestParseDateRange_InvalidEndFormat_ReturnsError(t *testing.T) {
	start, hasStart, endExcl, hasEnd, err := ParseDateRange(nil, sptr("Feb 3, 2026"))
	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if hasStart || hasEnd {
		t.Fatalf("expected hasStart/hasEnd false, got %v %v", hasStart, hasEnd)
	}
	if !start.IsZero() || !endExcl.IsZero() {
		t.Fatalf("expected zero times on error, got start=%v end=%v", start, endExcl)
	}
}

func TestParseDateRange_Reversed_DateOnly_DateOnly_SwapsAndEndExclusiveFromEndStr(t *testing.T) {
	startStr := "2026-02-10"
	endStr := "2026-02-01"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeDate(t, endStr)                      // swapped
	wantEndExcl := mustTimeDate(t, startStr).AddDate(0, 0, 1) // endStr is date-only => add 1 day to swapped rawEnd

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_Reversed_StartDateOnly_EndTimestamp_SwapsButDoesNotAddDay(t *testing.T) {
	startStr := "2026-02-10"
	endStr := "2026-02-01T12:00:00Z"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeRFC3339(t, endStr)  // swapped
	wantEndExcl := mustTimeDate(t, startStr) // endStr is timestamp => endExclusive is rawEnd, no +1 day

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_TrimSpaces_ParsesOK(t *testing.T) {
	startStr := " 2026-02-03 "
	endStr := " 2026-02-05T10:00:00Z "

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || !hasEnd {
		t.Fatalf("expected hasStart/hasEnd true, got %v %v", hasStart, hasEnd)
	}

	wantStart := mustTimeDate(t, "2026-02-03")
	wantEndExcl := mustTimeRFC3339(t, "2026-02-05T10:00:00Z")

	if !start.Equal(wantStart) {
		t.Fatalf("start mismatch: got=%v want=%v", start, wantStart)
	}
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}

func TestParseDateRange_OnlyStartProvided(t *testing.T) {
	startStr := "2026-02-03T10:00:00Z"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(sptr(startStr), nil)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if !hasStart || hasEnd {
		t.Fatalf("expected hasStart=true hasEnd=false, got %v %v", hasStart, hasEnd)
	}
	if !start.Equal(mustTimeRFC3339(t, startStr)) {
		t.Fatalf("start mismatch: got=%v", start)
	}
	if !endExcl.IsZero() {
		t.Fatalf("expected endExclusive zero, got %v", endExcl)
	}
}

func TestParseDateRange_OnlyEndProvided_DateOnlyAddsOneDay(t *testing.T) {
	endStr := "2026-02-03"

	start, hasStart, endExcl, hasEnd, err := ParseDateRange(nil, sptr(endStr))
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if hasStart || !hasEnd {
		t.Fatalf("expected hasStart=false hasEnd=true, got %v %v", hasStart, hasEnd)
	}
	if !start.IsZero() {
		t.Fatalf("expected start zero, got %v", start)
	}
	wantEndExcl := mustTimeDate(t, endStr).AddDate(0, 0, 1)
	if !endExcl.Equal(wantEndExcl) {
		t.Fatalf("endExclusive mismatch: got=%v want=%v", endExcl, wantEndExcl)
	}
}
