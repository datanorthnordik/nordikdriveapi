package main

import "testing"

func TestInferFieldRole(t *testing.T) {
	tests := []struct {
		field string
		want  string
	}{
		{field: "First Nation/Community", want: "community"},
		{field: "Residential School", want: "school"},
		{field: "Date of Death", want: "date"},
		{field: "Resident Name", want: "name"},
		{field: "Student Number", want: "identifier"},
		{field: "Death Cause", want: "cause"},
		{field: "Notes", want: "notes"},
	}

	for _, tt := range tests {
		if got := inferFieldRole(tt.field); got != tt.want {
			t.Fatalf("inferFieldRole(%q) = %q want %q", tt.field, got, tt.want)
		}
	}
}

func TestClassifyDateFormat(t *testing.T) {
	tests := []struct {
		value string
		want  string
	}{
		{value: "1905-04-06", want: "iso_date"},
		{value: "1905", want: "year_only"},
		{value: "1905-1906", want: "year_range"},
		{value: "04/06/1905", want: "slash_date"},
		{value: "06-04-1905", want: "dash_date"},
		{value: "April 6 1905", want: "month_name_date"},
		{value: "abt 1819", want: "approximate_text"},
		{value: "returned to school in spring 1905", want: "free_text"},
	}

	for _, tt := range tests {
		if got := classifyDateFormat(tt.value); got != tt.want {
			t.Fatalf("classifyDateFormat(%q) = %q want %q", tt.value, got, tt.want)
		}
	}
}

func TestHasSourceContamination(t *testing.T) {
	if !hasSourceContamination("Mary (NCTR SOURCE)") {
		t.Fatal("expected source contamination")
	}
	if !hasSourceContamination(`Mentioned in Indian Affairs. Letterbook, 17 October 1919-30 October 1919, (R.G. 10, Volume 5784) Reel c8980, image 254.`) {
		t.Fatal("expected archive citation contamination")
	}
	if hasSourceContamination("Mary Johnson") {
		t.Fatal("did not expect source contamination")
	}
}

func TestCollectVariantGroups(t *testing.T) {
	variants := map[string]map[string]struct{}{
		"garden river": {
			"Garden River":              {},
			"Garden River First Nation": {},
		},
		"single": {
			"single": {},
		},
	}

	groups := collectVariantGroups(variants)
	if len(groups) != 1 {
		t.Fatalf("expected 1 variant group, got %#v", groups)
	}
	if groups[0].Normalized != "garden river" {
		t.Fatalf("unexpected normalized value: %#v", groups[0])
	}
}
