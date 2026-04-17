package chat

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type temporalKind string

const (
	temporalKindUnknown     temporalKind = "unknown"
	temporalKindExactDay    temporalKind = "exact_day"
	temporalKindExactMonth  temporalKind = "exact_month"
	temporalKindExactYear   temporalKind = "exact_year"
	temporalKindApproximate temporalKind = "approximate"
	temporalKindRange       temporalKind = "range"
	temporalKindAlternative temporalKind = "alternative"
	temporalKindBefore      temporalKind = "before"
	temporalKindAfter       temporalKind = "after"
	temporalKindMalformed   temporalKind = "malformed"
)

type temporalValue struct {
	Raw          string         `json:"raw"`
	Kind         temporalKind   `json:"kind"`
	Precision    string         `json:"precision,omitempty"`
	Approximate  bool           `json:"approximate,omitempty"`
	Lower        *temporalBound `json:"lower,omitempty"`
	Upper        *temporalBound `json:"upper,omitempty"`
	Alternatives []temporalValue `json:"alternatives,omitempty"`
	Notes        []string       `json:"notes,omitempty"`
}

type temporalBound struct {
	Year  int `json:"year"`
	Month int `json:"month"`
	Day   int `json:"day"`
}

func (b temporalBound) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", b.Year, b.Month, b.Day)
}

func (b temporalBound) compare(other temporalBound) int {
	switch {
	case b.Year != other.Year:
		if b.Year < other.Year {
			return -1
		}
		return 1
	case b.Month != other.Month:
		if b.Month < other.Month {
			return -1
		}
		return 1
	case b.Day != other.Day:
		if b.Day < other.Day {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func (tv temporalValue) hasExactBounds() bool {
	return tv.Lower != nil && tv.Upper != nil
}

func (tv temporalValue) isDeterministic() bool {
	return !tv.Approximate && tv.Kind != temporalKindMalformed && tv.Kind != temporalKindUnknown && tv.Kind != temporalKindAlternative && tv.Kind != temporalKindBefore && tv.Kind != temporalKindAfter
}

func (tv temporalValue) definiteWithinRange(lower, upper temporalBound) bool {
	if !tv.hasExactBounds() || !tv.isDeterministic() {
		return false
	}
	return tv.Lower.compare(lower) >= 0 && tv.Upper.compare(upper) <= 0
}

func (tv temporalValue) definiteBefore(bound temporalBound) bool {
	if !tv.hasExactBounds() || !tv.isDeterministic() {
		return false
	}
	return tv.Upper.compare(bound) < 0
}

func (tv temporalValue) definiteAfter(bound temporalBound) bool {
	if !tv.hasExactBounds() || !tv.isDeterministic() {
		return false
	}
	return tv.Lower.compare(bound) > 0
}

var (
	textualMonthDateRe = regexp.MustCompile(`(?i)^\s*(\d{1,2})[\s\-\/]([A-Za-z]{3,9})[\s\-\/](\d{4})\s*$`)
	isoDayRe           = regexp.MustCompile(`^\s*(\d{4})\s*[-/ ]\s*(\d{2})\s*[-/ ]\s*(\d{2}|00|~)\s*$`)
	isoMonthRe         = regexp.MustCompile(`^\s*(\d{4})\s*[-/ ]\s*(\d{2}|00|~)\s*$`)
	yearOnlyRe         = regexp.MustCompile(`^\s*(\d{4})\s*$`)
	yearApproxRe       = regexp.MustCompile(`^\s*(\d{4})\s*~\s*$`)
	yearRangeRe        = regexp.MustCompile(`^\s*(\d{4})\s*[-/]\s*(\d{4})\s*$`)
	dateRangeRe        = regexp.MustCompile(`^\s*(.+?)\s*(?:to|through)\s*(.+?)\s*$`)
	beforeRe           = regexp.MustCompile(`(?i)^\s*(?:bef\.?|before)\s+(.+?)\s*$`)
	afterRe            = regexp.MustCompile(`(?i)^\s*(?:aft\.?|after)\s+(.+?)\s*$`)
	approxPrefixRe     = regexp.MustCompile(`(?i)^\s*(?:abt\.?|about|approx\.?|approximately|circa|ca\.?|c\.)\s+(.+?)\s*$`)
	alternativeSplitRe = regexp.MustCompile(`(?i)\s+(?:and|or)\s+`)
	embeddedDateRe     = regexp.MustCompile(`(?i)(\d{4}-\d{2}-\d{2}|\d{4}-\d{2}|\d{4}|\d{1,2}[\- ][A-Za-z]{3,9}[\- ]\d{4})`)
)

func parseTemporalValue(raw string) temporalValue {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return temporalValue{Raw: raw, Kind: temporalKindUnknown}
	}

	cleaned := normalizeTemporalInput(raw)
	if cleaned == "" {
		return temporalValue{Raw: raw, Kind: temporalKindUnknown}
	}

	if isTemporalUnknown(cleaned) {
		return temporalValue{Raw: raw, Kind: temporalKindUnknown}
	}

	if tv, ok := parseTemporalAlternative(raw, cleaned); ok {
		return tv
	}

	if matches := beforeRe.FindStringSubmatch(cleaned); len(matches) == 2 {
		base := parseTemporalValue(matches[1])
		tv := temporalValue{Raw: raw, Kind: temporalKindBefore, Precision: base.Precision}
		if base.Lower != nil {
			prev := previousDay(*base.Lower)
			tv.Upper = &prev
		}
		tv.Notes = append(tv.Notes, "before")
		return tv
	}

	if matches := afterRe.FindStringSubmatch(cleaned); len(matches) == 2 {
		base := parseTemporalValue(matches[1])
		tv := temporalValue{Raw: raw, Kind: temporalKindAfter, Precision: base.Precision}
		if base.Upper != nil {
			next := nextDay(*base.Upper)
			tv.Lower = &next
		}
		tv.Notes = append(tv.Notes, "after")
		return tv
	}

	if matches := approxPrefixRe.FindStringSubmatch(cleaned); len(matches) == 2 {
		base := parseTemporalValue(matches[1])
		base.Raw = raw
		base.Approximate = true
		if base.Kind == temporalKindExactDay || base.Kind == temporalKindExactMonth || base.Kind == temporalKindExactYear {
			base.Kind = temporalKindApproximate
		}
		return base
	}

	if matches := dateRangeRe.FindStringSubmatch(cleaned); len(matches) == 3 {
		left := parseTemporalValue(matches[1])
		right := parseTemporalValue(matches[2])
		if left.hasExactBounds() && right.hasExactBounds() {
			return temporalValue{
				Raw:       raw,
				Kind:      temporalKindRange,
				Precision: "range",
				Lower:     left.Lower,
				Upper:     right.Upper,
			}
		}
	}

	if matches := yearRangeRe.FindStringSubmatch(cleaned); len(matches) == 3 {
		y1, _ := strconv.Atoi(matches[1])
		y2, _ := strconv.Atoi(matches[2])
		if !validYear(y1) || !validYear(y2) || y2 < y1 {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid year range"}}
		}
		lower := temporalBound{Year: y1, Month: 1, Day: 1}
		upper := temporalBound{Year: y2, Month: 12, Day: 31}
		return temporalValue{Raw: raw, Kind: temporalKindRange, Precision: "range", Lower: &lower, Upper: &upper}
	}

	if strings.Contains(cleaned, "~") {
		noTilde := strings.ReplaceAll(cleaned, "~", "")
		base := parseTemporalValue(noTilde)
		base.Raw = raw
		base.Approximate = true
		if base.Kind != temporalKindMalformed && base.Kind != temporalKindUnknown {
			base.Kind = temporalKindApproximate
		}
		return base
	}

	if matches := textualMonthDateRe.FindStringSubmatch(cleaned); len(matches) == 4 {
		day, _ := strconv.Atoi(matches[1])
		month := parseMonthName(matches[2])
		year, _ := strconv.Atoi(matches[3])
		if !validYear(year) || month == 0 || !validDay(year, month, day) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid textual date"}}
		}
		lower := temporalBound{Year: year, Month: month, Day: day}
		upper := lower
		return temporalValue{Raw: raw, Kind: temporalKindExactDay, Precision: "day", Lower: &lower, Upper: &upper}
	}

	if matches := isoDayRe.FindStringSubmatch(cleaned); len(matches) == 4 {
		year, _ := strconv.Atoi(matches[1])
		monthToken := matches[2]
		dayToken := matches[3]
		if !validYear(year) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid year"}}
		}
		if monthToken == "00" || monthToken == "~" {
			lower := temporalBound{Year: year, Month: 1, Day: 1}
			upper := temporalBound{Year: year, Month: 12, Day: 31}
			return temporalValue{Raw: raw, Kind: temporalKindExactYear, Precision: "year", Lower: &lower, Upper: &upper}
		}
		month, _ := strconv.Atoi(monthToken)
		if !validMonth(month) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid month"}}
		}
		if dayToken == "00" || dayToken == "~" {
			lower := temporalBound{Year: year, Month: month, Day: 1}
			upper := temporalBound{Year: year, Month: month, Day: lastDayOfMonth(year, month)}
			return temporalValue{Raw: raw, Kind: temporalKindExactMonth, Precision: "month", Lower: &lower, Upper: &upper}
		}
		day, _ := strconv.Atoi(dayToken)
		if !validDay(year, month, day) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid day"}}
		}
		lower := temporalBound{Year: year, Month: month, Day: day}
		upper := lower
		return temporalValue{Raw: raw, Kind: temporalKindExactDay, Precision: "day", Lower: &lower, Upper: &upper}
	}

	if matches := isoMonthRe.FindStringSubmatch(cleaned); len(matches) == 3 {
		year, _ := strconv.Atoi(matches[1])
		monthToken := matches[2]
		if !validYear(year) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid year"}}
		}
		if monthToken == "00" || monthToken == "~" {
			lower := temporalBound{Year: year, Month: 1, Day: 1}
			upper := temporalBound{Year: year, Month: 12, Day: 31}
			return temporalValue{Raw: raw, Kind: temporalKindExactYear, Precision: "year", Lower: &lower, Upper: &upper}
		}
		month, _ := strconv.Atoi(monthToken)
		if !validMonth(month) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid month"}}
		}
		lower := temporalBound{Year: year, Month: month, Day: 1}
		upper := temporalBound{Year: year, Month: month, Day: lastDayOfMonth(year, month)}
		return temporalValue{Raw: raw, Kind: temporalKindExactMonth, Precision: "month", Lower: &lower, Upper: &upper}
	}

	if matches := yearApproxRe.FindStringSubmatch(cleaned); len(matches) == 2 {
		year, _ := strconv.Atoi(matches[1])
		if !validYear(year) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid year"}}
		}
		lower := temporalBound{Year: year, Month: 1, Day: 1}
		upper := temporalBound{Year: year, Month: 12, Day: 31}
		return temporalValue{Raw: raw, Kind: temporalKindApproximate, Precision: "year", Approximate: true, Lower: &lower, Upper: &upper}
	}

	if matches := yearOnlyRe.FindStringSubmatch(cleaned); len(matches) == 2 {
		year, _ := strconv.Atoi(matches[1])
		if !validYear(year) {
			return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"invalid year"}}
		}
		lower := temporalBound{Year: year, Month: 1, Day: 1}
		upper := temporalBound{Year: year, Month: 12, Day: 31}
		return temporalValue{Raw: raw, Kind: temporalKindExactYear, Precision: "year", Lower: &lower, Upper: &upper}
	}

	if extractEmbeddedDates(cleaned) != nil {
		if tv, ok := parseTemporalAlternative(raw, cleaned); ok {
			return tv
		}
	}

	return temporalValue{Raw: raw, Kind: temporalKindMalformed, Notes: []string{"unrecognized date format"}}
}

func parseTemporalAlternative(raw, cleaned string) (temporalValue, bool) {
	candidates := extractEmbeddedDates(cleaned)
	if len(candidates) < 2 {
		return temporalValue{}, false
	}

	seen := map[string]struct{}{}
	values := make([]temporalValue, 0, len(candidates))
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		parsed := parseTemporalValue(candidate)
		if parsed.Kind == temporalKindMalformed || parsed.Kind == temporalKindUnknown {
			continue
		}
		values = append(values, parsed)
	}
	if len(values) < 2 {
		return temporalValue{}, false
	}

	lower := values[0].Lower
	upper := values[0].Upper
	for _, value := range values[1:] {
		if value.Lower != nil && lower != nil && value.Lower.compare(*lower) < 0 {
			lower = value.Lower
		}
		if value.Upper != nil && upper != nil && value.Upper.compare(*upper) > 0 {
			upper = value.Upper
		}
	}
	return temporalValue{
		Raw:          raw,
		Kind:         temporalKindAlternative,
		Precision:    "alternative",
		Lower:        lower,
		Upper:        upper,
		Alternatives: values,
		Notes:        []string{"multiple candidate dates"},
	}, true
}

func normalizeTemporalInput(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.ReplaceAll(raw, "\r", " ")
	raw = strings.ReplaceAll(raw, "\n", " ")
	raw = strings.ReplaceAll(raw, "\t", " ")
	raw = strings.ReplaceAll(raw, "–", "-")
	raw = strings.ReplaceAll(raw, "—", "-")
	raw = strings.ReplaceAll(raw, "  ", " ")
	raw = strings.TrimSpace(raw)
	return raw
}

func isTemporalUnknown(v string) bool {
	v = strings.ToLower(strings.TrimSpace(v))
	switch v {
	case "", "unknown", "unk", "n/a", "na", "not known":
		return true
	}
	return false
}

func extractEmbeddedDates(v string) []string {
	if strings.Contains(v, "(") || alternativeSplitRe.MatchString(v) {
		matches := embeddedDateRe.FindAllString(v, -1)
		if len(matches) > 0 {
			return matches
		}
	}
	return nil
}

func parseMonthName(name string) int {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "jan", "january":
		return 1
	case "feb", "february":
		return 2
	case "mar", "march":
		return 3
	case "apr", "april":
		return 4
	case "may":
		return 5
	case "jun", "june":
		return 6
	case "jul", "july":
		return 7
	case "aug", "august":
		return 8
	case "sep", "sept", "september":
		return 9
	case "oct", "october":
		return 10
	case "nov", "november":
		return 11
	case "dec", "december":
		return 12
	default:
		return 0
	}
}

func validYear(year int) bool {
	return year >= 1600 && year <= 2100
}

func validMonth(month int) bool {
	return month >= 1 && month <= 12
}

func validDay(year, month, day int) bool {
	if !validMonth(month) || day <= 0 {
		return false
	}
	return day <= lastDayOfMonth(year, month)
}

func lastDayOfMonth(year, month int) int {
	if !validYear(year) || !validMonth(month) {
		return 31
	}
	t := time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC)
	return t.Day()
}

func previousDay(bound temporalBound) temporalBound {
	t := time.Date(bound.Year, time.Month(bound.Month), bound.Day, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -1)
	return temporalBound{Year: t.Year(), Month: int(t.Month()), Day: t.Day()}
}

func nextDay(bound temporalBound) temporalBound {
	t := time.Date(bound.Year, time.Month(bound.Month), bound.Day, 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return temporalBound{Year: t.Year(), Month: int(t.Month()), Day: t.Day()}
}

func renderTemporalMetadata(tv temporalValue) map[string]any {
	out := map[string]any{
		"raw":  tv.Raw,
		"kind": string(tv.Kind),
	}
	if tv.Precision != "" {
		out["precision"] = tv.Precision
	}
	if tv.Approximate {
		out["approximate"] = true
	}
	if tv.Lower != nil {
		out["lower"] = tv.Lower.String()
	}
	if tv.Upper != nil {
		out["upper"] = tv.Upper.String()
	}
	if len(tv.Notes) > 0 {
		out["notes"] = cloneStringSlice(tv.Notes)
	}
	if len(tv.Alternatives) > 0 {
		alts := make([]map[string]any, 0, len(tv.Alternatives))
		for _, alt := range tv.Alternatives {
			alts = append(alts, renderTemporalMetadata(alt))
		}
		out["alternatives"] = alts
	}
	return out
}
