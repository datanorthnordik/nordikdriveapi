package util

import (
	"reflect"
	"testing"
)

func TestParseCommaSeparatedCommunities_EmptyValues_ReturnsNil(t *testing.T) {
	got := ParseCommaSeparatedCommunities(nil)
	if got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

func TestParseCommaSeparatedCommunities_FirstElementEmpty_ReturnsNil(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{""})
	if got != nil {
		t.Fatalf("expected nil, got %#v", got)
	}
}

func TestParseCommaSeparatedCommunities_IgnoresAdditionalElements(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{"c1,c2", "c3"})
	want := []string{"c1", "c2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestParseCommaSeparatedCommunities_SplitsAndTrims(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{" c1 , c2,  c3 "})
	want := []string{"c1", "c2", "c3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestParseCommaSeparatedCommunities_RemovesEmptyParts(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{"c1,, ,c2, , ,c3,"})
	want := []string{"c1", "c2", "c3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestParseCommaSeparatedCommunities_PreservesQuotes(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{`"Shingwauk","Garden River", "Batchewana"`})
	want := []string{`"Shingwauk"`, `"Garden River"`, `"Batchewana"`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestParseCommaSeparatedCommunities_SingleValueNoComma(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{"c1"})
	want := []string{"c1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v want %#v", got, want)
	}
}

func TestParseCommaSeparatedCommunities_AllSpacesAfterSplit_ReturnsEmptySlice(t *testing.T) {
	got := ParseCommaSeparatedCommunities([]string{" , ,  ,"})
	if got == nil {
		t.Fatalf("expected empty slice (not nil), got nil")
	}
	if len(got) != 0 {
		t.Fatalf("expected empty slice, got %#v", got)
	}
}
