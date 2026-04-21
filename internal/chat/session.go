package chat

import (
	"fmt"
	"strings"
	"time"
)

const (
	chatSessionTTL          = 45 * time.Minute
	chatSessionMaxTurns     = 6
	chatClarificationBudget = 2
)

type chatPendingClarification struct {
	Prompt           string
	Attempts         int
	OriginalQuestion string
}

type chatSessionState struct {
	UserKey     string
	FileID      uint
	Filename    string
	Version     int
	Communities []string
	Pending     *chatPendingClarification
	RecentTurns []chatSessionTurn
	UpdatedAt   time.Time
}

type chatSessionTurn struct {
	Question string
	Answer   string
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

func clonePending(src *chatPendingClarification) *chatPendingClarification {
	if src == nil {
		return nil
	}
	return &chatPendingClarification{
		Prompt:           src.Prompt,
		Attempts:         src.Attempts,
		OriginalQuestion: src.OriginalQuestion,
	}
}

func cloneSessionState(src *chatSessionState) *chatSessionState {
	if src == nil {
		return nil
	}
	return &chatSessionState{
		UserKey:     src.UserKey,
		FileID:      src.FileID,
		Filename:    src.Filename,
		Version:     src.Version,
		Communities: cloneStringSlice(src.Communities),
		Pending:     clonePending(src.Pending),
		RecentTurns: cloneTurns(src.RecentTurns),
		UpdatedAt:   src.UpdatedAt,
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

func (s *chatSessionState) registerTurn(question, answer string) {
	if s == nil {
		return
	}
	s.UpdatedAt = time.Now().UTC()

	question = strings.TrimSpace(question)
	answer = strings.TrimSpace(answer)
	if question == "" && answer == "" {
		return
	}

	s.RecentTurns = append(s.RecentTurns, chatSessionTurn{
		Question: question,
		Answer:   answer,
		At:       s.UpdatedAt,
	})
	if len(s.RecentTurns) > chatSessionMaxTurns {
		s.RecentTurns = s.RecentTurns[len(s.RecentTurns)-chatSessionMaxTurns:]
	}
}

func (s *chatSessionState) summaryForPrompt() string {
	if s == nil || len(s.RecentTurns) == 0 {
		return "No recent conversation context."
	}

	start := 0
	if len(s.RecentTurns) > 2 {
		start = len(s.RecentTurns) - 2
	}

	lines := []string{"Recent conversation:"}
	for _, turn := range s.RecentTurns[start:] {
		question := strings.TrimSpace(turn.Question)
		answer := strings.TrimSpace(turn.Answer)
		if question == "" {
			continue
		}
		if len(question) > 180 {
			question = question[:177] + "..."
		}
		if len(answer) > 220 {
			answer = answer[:217] + "..."
		}
		lines = append(lines, fmt.Sprintf("- Q: %s", question))
		if answer != "" {
			lines = append(lines, fmt.Sprintf("- A: %s", answer))
		}
	}

	if len(lines) == 1 {
		return "No recent conversation context."
	}
	return strings.Join(lines, "\n")
}
