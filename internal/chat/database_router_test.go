package chat

import (
	"context"
	"testing"

	"google.golang.org/genai"
)

// TestChatService_Chat_ReusesFilteredAggregationAcrossPhrasings proves the
// database aggregation memoization: two different phrasings that resolve to the
// same filtered count must run the aggregation query only once. If the second
// call issues a new count query, sqlmock has no expectation for it and the test
// fails.
func TestChatService_Chat_ReusesFilteredAggregationAcrossPhrasings(t *testing.T) {
	db, mock, cleanup := newMockDBChatSvc(t)
	defer cleanup()

	expectChatFileLookup(mock, "sheet.xlsx")
	expectDatabaseDimensionValues(mock, "community",
		databaseDimensionValue{Normalized: "garden river", Display: "Garden River", Count: 2},
	)
	expectDatabaseDimensionValues(mock, "school",
		databaseDimensionValue{Normalized: "shingwauk", Display: "Shingwauk", Count: 3},
	)
	expectDatabaseCount(mock, 2)
	// Only the per-request file lookup repeats on the second, differently
	// phrased question; the community/school dimensions and the count are
	// served from the database aggregation cache.
	expectChatFileLookup(mock, "sheet.xlsx")

	oldGenerate := genaiGenerateContentHook
	t.Cleanup(func() {
		genaiGenerateContentHook = oldGenerate
	})
	modelCalls := 0
	genaiGenerateContentHook = func(_ *genai.Client, _ context.Context, _ string, contents []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		modelCalls++
		return verifiedRoutedAnswer(t, contents), nil
	}

	cs := &ChatService{DB: db, Client: &genai.Client{}}
	first, err := cs.Chat("How many students are from Garden River?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("first chat: %v", err)
	}
	second, err := cs.Chat("How many students are in Garden River?", nil, "sheet.xlsx", nil)
	if err != nil {
		t.Fatalf("second chat: %v", err)
	}

	if first.Debug == nil || first.Debug.RetrievalMode != "database_count" {
		t.Fatalf("first route = %#v want database_count", first.Debug)
	}
	if second.Debug == nil || second.Debug.RetrievalMode != "database_count" {
		t.Fatalf("second route = %#v want database_count", second.Debug)
	}
	if second.Debug.Strategy == "fast_answer_cache" {
		t.Fatal("second differently phrased question must not hit the exact-question answer cache")
	}
	if first.Answer != second.Answer {
		t.Fatalf("answer changed across phrasings: first=%q second=%q", first.Answer, second.Answer)
	}
	if modelCalls != 2 {
		t.Fatalf("routed answer model calls = %d want 2 (one per question)", modelCalls)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// TestChatService_DatabaseAggregations_AreMemoized exercises each memoized
// aggregation directly: calling it twice with the same pinned inputs must run
// the underlying query once and return identical results.
func TestChatService_DatabaseAggregations_AreMemoized(t *testing.T) {
	input := ChatQueryInput{FileID: 1, Version: 1, FileName: "sheet.xlsx", Communities: []string{"Garden River"}}

	t.Run("dimension counts", func(t *testing.T) {
		db, mock, cleanup := newMockDBChatSvc(t)
		defer cleanup()
		expectDatabaseDimensionValues(mock, "community",
			databaseDimensionValue{Normalized: "garden river", Display: "Garden River", Count: 2},
		)

		cs := &ChatService{DB: db}
		first, err := cs.queryDatabaseDimensionCounts(input, "community", deterministicRowFilter{CommunityNormalized: "garden river"})
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		second, err := cs.queryDatabaseDimensionCounts(input, "community", deterministicRowFilter{CommunityNormalized: "garden river"})
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if len(first) != 1 || len(second) != 1 || first[0] != second[0] {
			t.Fatalf("cached dimension values differ: first=%#v second=%#v", first, second)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("count", func(t *testing.T) {
		db, mock, cleanup := newMockDBChatSvc(t)
		defer cleanup()
		expectDatabaseCount(mock, 2)

		cs := &ChatService{DB: db}
		first, err := cs.countDatabaseRows(input, deterministicRowFilter{CommunityNormalized: "garden river"})
		if err != nil || first != 2 {
			t.Fatalf("first = %d, %v want 2, nil", first, err)
		}
		second, err := cs.countDatabaseRows(input, deterministicRowFilter{CommunityNormalized: "garden river"})
		if err != nil || second != 2 {
			t.Fatalf("second = %d, %v want 2, nil", second, err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("named records", func(t *testing.T) {
		db, mock, cleanup := newMockDBChatSvc(t)
		defer cleanup()
		expectDatabaseNamedRecords(mock, databaseNamedRecord{SourceRowID: 1, DisplayName: "Alice"})

		cs := &ChatService{DB: db}
		first, err := cs.queryDatabaseNamedRecords(input, deterministicRowFilter{})
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		second, err := cs.queryDatabaseNamedRecords(input, deterministicRowFilter{})
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if len(first) != 1 || len(second) != 1 || first[0] != second[0] {
			t.Fatalf("cached records differ: first=%#v second=%#v", first, second)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("configured distinct values", func(t *testing.T) {
		db, mock, cleanup := newMockDBChatSvc(t)
		defer cleanup()
		expectDatabaseConfiguredValues(mock, databaseConfiguredValue{Display: "Garden River", Count: 2})

		cs := &ChatService{DB: db}
		field := configuredDeterministicField{Key: "band", SourceNames: []string{"Band"}}
		firstValues, firstRows, err := cs.queryConfiguredDatabaseDistinctValues(input, field, deterministicRowFilter{})
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		secondValues, secondRows, err := cs.queryConfiguredDatabaseDistinctValues(input, field, deterministicRowFilter{})
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if firstRows != secondRows || len(firstValues) != len(secondValues) || firstValues[0] != secondValues[0] {
			t.Fatalf("cached distinct values differ: first=%#v/%d second=%#v/%d", firstValues, firstRows, secondValues, secondRows)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}
