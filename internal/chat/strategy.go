package chat

import (
	"fmt"
	"strings"
	"time"
)

type ChatQueryInput struct {
	FileID      uint
	Version     int
	Question    string
	Communities []string
}

type PreparedChatPrompt struct {
	Prompt     string
	RowRefToID map[string]int
	Debug      ChatDebugMetrics
}

type ChatQueryStrategy interface {
	Name() string
	Prepare(cs *ChatService, input ChatQueryInput) (*PreparedChatPrompt, error)
}

type fullDatasetChatStrategy struct{}
type structuredRetrievalChatStrategy struct{}

func (fullDatasetChatStrategy) Name() string {
	return "full_dataset"
}

func (structuredRetrievalChatStrategy) Name() string {
	return "structured_retrieval"
}

func (structuredRetrievalChatStrategy) Prepare(cs *ChatService, input ChatQueryInput) (*PreparedChatPrompt, error) {
	start := time.Now()

	filtered := normalizeCommunities(input.Communities)
	prepared, err := cs.getPreparedStructuredChatDataset(input.FileID, input.Version, strings.TrimSpace(input.Question), filtered)
	if err != nil {
		fallback, fallbackErr := (fullDatasetChatStrategy{}).Prepare(cs, input)
		if fallbackErr != nil {
			return nil, fallbackErr
		}
		fallback.Debug.Strategy = (structuredRetrievalChatStrategy{}).Name()
		fallback.Debug.RetrievalMode = "legacy_full_dataset_fallback"
		fallback.Debug.PreparationMillis = time.Since(start).Milliseconds()
		fallback.Debug.PromptChars = len(fallback.Prompt)
		fallback.Debug.PromptBytes = len([]byte(fallback.Prompt))
		return fallback, nil
	}

	prompt := fmt.Sprintf(
		"%s\n\nStructured output requirements:\n%s\n\nData notes:\n%s\n\nUser question:\n%s\n\nDATA (only source of truth):\n%s",
		strings.TrimSpace(chatStyleInstruction),
		strings.TrimSpace(chatStructuredOutputInstruction),
		strings.TrimSpace(chatCompactDataInstruction),
		strings.TrimSpace(input.Question),
		prepared.PromptJSON,
	)

	return &PreparedChatPrompt{
		Prompt:     prompt,
		RowRefToID: prepared.RowRefToID,
		Debug: ChatDebugMetrics{
			Strategy:              (structuredRetrievalChatStrategy{}).Name(),
			RetrievalMode:         prepared.RetrievalMode,
			PromptProjectionMode:  prepared.PromptProjectionMode,
			Version:               input.Version,
			CommunityFilterCount:  len(filtered),
			TotalRowsLoaded:       prepared.TotalRows,
			RowsSelected:          prepared.SelectedRows,
			NarrativeRowsIncluded: prepared.NarrativeRows,
			PromptChars:           len(prompt),
			PromptBytes:           len([]byte(prompt)),
			PreparationMillis:     time.Since(start).Milliseconds(),
		},
	}, nil
}

func (fullDatasetChatStrategy) Prepare(cs *ChatService, input ChatQueryInput) (*PreparedChatPrompt, error) {
	start := time.Now()

	filtered := normalizeCommunities(input.Communities)
	prepared, err := cs.getPreparedChatDataset(input.FileID, input.Version, filtered)
	if err != nil {
		return nil, err
	}

	prompt := fmt.Sprintf(
		"%s\n\nStructured output requirements:\n%s\n\nUser question:\n%s\n\nDATA (only source of truth):\n%s",
		strings.TrimSpace(chatStyleInstruction),
		strings.TrimSpace(chatStructuredOutputInstruction),
		strings.TrimSpace(input.Question),
		prepared.PromptJSON,
	)

	return &PreparedChatPrompt{
		Prompt:     prompt,
		RowRefToID: prepared.RowRefToID,
		Debug: ChatDebugMetrics{
			Strategy:             (fullDatasetChatStrategy{}).Name(),
			PromptProjectionMode: "full_dataset",
			Version:              input.Version,
			CommunityFilterCount: len(filtered),
			TotalRowsLoaded:      prepared.TotalRows,
			RowsSelected:         prepared.SelectedRows,
			PromptChars:          len(prompt),
			PromptBytes:          len([]byte(prompt)),
			PreparationMillis:    time.Since(start).Milliseconds(),
		},
	}, nil
}
