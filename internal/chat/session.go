package chat

import (
	"fmt"
	"strings"
	"time"
)

const (
	chatSessionTTL      = 45 * time.Minute
	chatSessionMaxTurns = 6
)

type chatSessionState struct {
	UserKey       string
	FileID        uint
	Filename      string
	Version       int
	Communities   []string
	FocusRowIDs   []int
	LastFieldID   string
	PendingPrompt string
	RecentTurns   []chatSessionTurn
	UpdatedAt     time.Time
}

type chatSessionTurn struct {
	Question string
	Answer   string
	FieldID  string
	At       time.Time
}

func chatSessionKey(userID int64) string {
	return fmt.Sprintf("u:%d", userID)
}

func communitiesSignature(communities []string) string {
	if len(communities) == 0 {
		return "__all__"
	}
	return strings.Join(communities, "\x1f")
}

func cloneIntSlice(src []int) []int {
	if len(src) == 0 {
		return nil
	}
	out := make([]int, len(src))
	copy(out, src)
	return out
}

func cloneStringSlice(src []string) []string {
	if len(src) == 0 {
		return nil
	}
	out := make([]string, len(src))
	copy(out, src)
	return out
}

func cloneTurns(src []chatSessionTurn) []chatSessionTurn {
	if len(src) == 0 {
		return nil
	}
	out := make([]chatSessionTurn, len(src))
	copy(out, src)
	return out
}

func cloneSessionState(src *chatSessionState) *chatSessionState {
	if src == nil {
		return nil
	}
	return &chatSessionState{
		UserKey:       src.UserKey,
		FileID:        src.FileID,
		Filename:      src.Filename,
		Version:       src.Version,
		Communities:   cloneStringSlice(src.Communities),
		FocusRowIDs:   cloneIntSlice(src.FocusRowIDs),
		LastFieldID:   src.LastFieldID,
		PendingPrompt: src.PendingPrompt,
		RecentTurns:   cloneTurns(src.RecentTurns),
		UpdatedAt:     src.UpdatedAt,
	}
}

func (cs *ChatService) loadSession(userID int64, fileID uint, filename string, version int, communities []string) *chatSessionState {
	if userID <= 0 {
		return &chatSessionState{
			UserKey:     chatSessionKey(userID),
			FileID:      fileID,
			Filename:    filename,
			Version:     version,
			Communities: cloneStringSlice(communities),
			UpdatedAt:   time.Now().UTC(),
		}
	}

	key := chatSessionKey(userID)
	if raw, ok := cs.sessionCache.Load(key); ok {
		if state, ok := raw.(*chatSessionState); ok {
			if time.Since(state.UpdatedAt) <= chatSessionTTL &&
				state.FileID == fileID &&
				state.Version == version &&
				communitiesSignature(state.Communities) == communitiesSignature(communities) {
				cloned := cloneSessionState(state)
				cloned.UpdatedAt = time.Now().UTC()
				return cloned
			}
			cs.sessionCache.Delete(key)
		}
	}

	return &chatSessionState{
		UserKey:     key,
		FileID:      fileID,
		Filename:    filename,
		Version:     version,
		Communities: cloneStringSlice(communities),
		UpdatedAt:   time.Now().UTC(),
	}
}

func (cs *ChatService) saveSession(userID int64, state *chatSessionState) {
	if userID <= 0 || state == nil {
		return
	}
	state.UpdatedAt = time.Now().UTC()
	cs.sessionCache.Store(chatSessionKey(userID), cloneSessionState(state))
}

func (s *chatSessionState) registerTurn(question, answer, fieldID string, focusRowIDs []int) {
	if s == nil {
		return
	}
	s.UpdatedAt = time.Now().UTC()
	s.LastFieldID = strings.TrimSpace(fieldID)
	s.FocusRowIDs = cloneIntSlice(focusRowIDs)
	question = strings.TrimSpace(question)
	answer = strings.TrimSpace(answer)
	if question == "" && answer == "" {
		return
	}

	s.RecentTurns = append(s.RecentTurns, chatSessionTurn{
		Question: question,
		Answer:   answer,
		FieldID:  s.LastFieldID,
		At:       s.UpdatedAt,
	})
	if len(s.RecentTurns) > chatSessionMaxTurns {
		s.RecentTurns = s.RecentTurns[len(s.RecentTurns)-chatSessionMaxTurns:]
	}
}

func (s *chatSessionState) summaryForPrompt(dataset *chatDatasetCacheEntry) string {
	if s == nil {
		return "No active session context."
	}

	lines := []string{}
	if len(s.FocusRowIDs) > 0 {
		focusNames := make([]string, 0, len(s.FocusRowIDs))
		for _, rowID := range s.FocusRowIDs {
			if row, ok := dataset.rowByID[rowID]; ok {
				name := strings.TrimSpace(row.primaryName())
				if name == "" {
					name = fmt.Sprintf("row %d", rowID)
				}
				focusNames = append(focusNames, name)
			}
		}
		if len(focusNames) > 0 {
			lines = append(lines, "Current focus: "+strings.Join(focusNames, ", "))
		}
	}
	if s.LastFieldID != "" {
		lines = append(lines, "Last field: "+s.LastFieldID)
	}
	if s.PendingPrompt != "" {
		lines = append(lines, "Pending clarification: "+s.PendingPrompt)
	}
	if len(s.RecentTurns) > 0 {
		lines = append(lines, "Recent turns:")
		start := 0
		if len(s.RecentTurns) > 3 {
			start = len(s.RecentTurns) - 3
		}
		for _, turn := range s.RecentTurns[start:] {
			q := strings.TrimSpace(turn.Question)
			a := strings.TrimSpace(turn.Answer)
			if q == "" {
				continue
			}
			if len(q) > 160 {
				q = q[:157] + "..."
			}
			if len(a) > 200 {
				a = a[:197] + "..."
			}
			lines = append(lines, fmt.Sprintf("- Q: %s | A: %s", q, a))
		}
	}
	if len(lines) == 0 {
		return "No active session context."
	}
	return strings.Join(lines, "\n")
}
