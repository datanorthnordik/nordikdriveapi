package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"strconv"
	"strings"
	"sync"
	"unicode"

	f "nordik-drive-api/internal/file"
)

type chatDatasetCacheEntry struct {
	rows     []cachedChatRow
	prepared sync.Map
}

type cachedChatRow struct {
	RowID     int
	RowJSON   string
	Community string
}

type preparedChatDataset struct {
	PromptJSON string
	RowRefToID map[string]int
}

type chatStructuredResponse struct {
	Answer        string  `json:"answer"`
	MatchedRowRef *string `json:"matched_row_ref"`
}

const chatStyleInstruction = `
You are a helpful assistant answering a community data question for a non-technical user.

Style requirements:
- Answer like a human: natural, warm, and conversational.
- Prefer short paragraphs over bullet points.
- Do NOT use bullet points unless the user explicitly asks for a list.
- Do NOT sound robotic or overly formal.
- Do NOT mention JSON, database, columns, file name, file version, or any technical details.
- If the answer is not present in the provided data, say so clearly and ask 1 short follow-up question if needed.

Accuracy requirements:
- Use ONLY the provided data below. Do not use outside knowledge.
- Carefully analyze ALL provided data before answering.
- Do not stop after finding the first match.
- Always check if multiple equally correct answers exist.
- Never return only one answer if multiple valid answers are present.
- For questions involving highest, lowest, most, least, first, last, top, or similar comparisons:
  verify that no other entries share the same value before answering.

Before responding:
- Internally re-check the data to confirm whether another valid answer exists.
- Only respond after confirming that all correct answers are included.

Answer format:
- Start with a direct answer in 1-2 sentences.
- If multiple answers exist, include ALL of them in the first sentence.
- Combine them naturally (example: "1882 and 1995").
- Provide as much detail as possible based on the data.
`

const chatStructuredOutputInstruction = `
Return ONLY a single JSON object with this shape:
{
  "answer": "your natural-language answer",
  "matched_row_ref": "R1" or null
}

Rules for matched_row_ref:
- Set it only when the user's question is about a person and exactly one row is the best match.
- You may also set it when your answer is based entirely on one single row, even if the question is not explicitly about a person.
- If the answer depends on multiple rows, or more than one row could match, set matched_row_ref to null.
- If a person's name appears misspelled but the intended single row is still clear from the data, you may still choose that row_ref.
- Use only row_ref values that appear in the DATA.
- Never invent, guess, or approximate a row_ref.
`

func (cs *ChatService) Chat(question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	if cs.DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	if cs.Client == nil {
		return nil, fmt.Errorf("genai client not initialized")
	}

	var file f.File
	if err := cs.DB.Select("id, version").Where("filename = ?", filename).Order("version DESC").First(&file).Error; err != nil {
		return nil, fmt.Errorf("file not found")
	}

	filtered := normalizeCommunities(communities)
	prepared, err := cs.getPreparedChatDataset(file.ID, file.Version, filtered)
	if err != nil {
		return nil, err
	}

	prompt := fmt.Sprintf(
		"%s\n\nStructured output requirements:\n%s\n\nUser question:\n%s\n\nDATA (only source of truth):\n%s",
		strings.TrimSpace(chatStyleInstruction),
		strings.TrimSpace(chatStructuredOutputInstruction),
		strings.TrimSpace(question),
		prepared.PromptJSON,
	)

	ctx := context.Background()

	var audioBytes []byte
	var audioMime string
	if audioFile != nil {
		fh, err := openMultipartFileHook(audioFile)
		if err != nil {
			return nil, fmt.Errorf("failed to open audio file: %w", err)
		}
		defer fh.Close()

		audioBytes, err = readAllHook(fh)
		if err != nil {
			return nil, fmt.Errorf("failed to read audio file: %w", err)
		}

		audioMime = audioFile.Header.Get("Content-Type")
	}

	answer, usedModel, err := cs.generateFromPrompt(ctx, prompt, audioBytes, audioMime)
	if err != nil {
		return nil, fmt.Errorf("generation error (%s): %w", usedModel, err)
	}

	resolvedAnswer, matchedRowID := resolveChatResponse(answer, prepared.RowRefToID)
	return &ChatResult{
		Answer:       resolvedAnswer,
		MatchedRowID: matchedRowID,
	}, nil
}

func (cs *ChatService) ChatForUser(_ int64, question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	return cs.Chat(question, audioFile, filename, communities)
}

func chatDatasetCacheKey(fileID uint, version int) string {
	return fmt.Sprintf("%d:%d", fileID, version)
}

func chatCommunitiesCacheKey(communities []string) string {
	if len(communities) == 0 {
		return "__all__"
	}
	return strings.Join(communities, "\x1f")
}

func (cs *ChatService) getPreparedChatDataset(fileID uint, version int, communities []string) (*preparedChatDataset, error) {
	dataset, err := cs.getOrLoadChatDataset(fileID, version)
	if err != nil {
		return nil, err
	}

	cacheKey := chatCommunitiesCacheKey(communities)
	if cached, ok := dataset.prepared.Load(cacheKey); ok {
		if prepared, ok := cached.(*preparedChatDataset); ok {
			return prepared, nil
		}
	}

	prepared, err := buildPreparedChatDataset(dataset.rows, communities)
	if err != nil {
		return nil, err
	}

	actual, _ := dataset.prepared.LoadOrStore(cacheKey, prepared)
	if cached, ok := actual.(*preparedChatDataset); ok {
		return cached, nil
	}

	return prepared, nil
}

func (cs *ChatService) getOrLoadChatDataset(fileID uint, version int) (*chatDatasetCacheEntry, error) {
	cacheKey := chatDatasetCacheKey(fileID, version)
	if cached, ok := cs.datasetCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatDatasetCacheEntry); ok {
			return entry, nil
		}
	}

	var rawRows []f.FileData
	if err := cs.DB.Select("id, row_data").
		Where("file_id = ? AND version = ?", fileID, version).
		Order("id ASC").
		Find(&rawRows).Error; err != nil {
		return nil, fmt.Errorf("file data not found: %w", err)
	}

	rows := make([]cachedChatRow, 0, len(rawRows))
	for _, rawRow := range rawRows {
		rows = append(rows, buildCachedChatRow(rawRow))
	}

	entry := &chatDatasetCacheEntry{rows: rows}
	actual, _ := cs.datasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatDatasetCacheEntry); ok {
		return cached, nil
	}

	return entry, nil
}

func buildCachedChatRow(rawRow f.FileData) cachedChatRow {
	row := cachedChatRow{
		RowID:   int(rawRow.ID),
		RowJSON: string(rawRow.RowData),
	}

	var rowMap map[string]interface{}
	if err := json.Unmarshal(rawRow.RowData, &rowMap); err != nil {
		return row
	}

	row.Community = extractCommunityValue(rowMap)
	return row
}

func extractCommunityValue(rowMap map[string]interface{}) string {
	rawVal, ok := rowMap["First Nation/Community"]
	if !ok || rawVal == nil {
		return ""
	}

	valStr, ok := rawVal.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(valStr)
}

func buildPreparedChatDataset(rows []cachedChatRow, communities []string) (*preparedChatDataset, error) {
	selectedIndexes := make([]int, 0, len(rows))
	if len(communities) == 0 {
		for idx := range rows {
			selectedIndexes = append(selectedIndexes, idx)
		}
	} else {
		allowed := make(map[string]struct{}, len(communities))
		for _, community := range communities {
			allowed[community] = struct{}{}
		}
		for idx, row := range rows {
			if _, ok := allowed[row.Community]; ok {
				selectedIndexes = append(selectedIndexes, idx)
			}
		}
	}

	promptJSON, rowRefToID, err := buildPromptJSONArray(rows, selectedIndexes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file data: %w", err)
	}

	return &preparedChatDataset{
		PromptJSON: promptJSON,
		RowRefToID: rowRefToID,
	}, nil
}

func buildPromptJSONArray(rows []cachedChatRow, indexes []int) (string, map[string]int, error) {
	if len(indexes) == 0 {
		return "[]", map[string]int{}, nil
	}

	var b strings.Builder
	rowRefToID := make(map[string]int, len(indexes))
	b.WriteByte('[')
	for idx, rowIndex := range indexes {
		rowJSON := rows[rowIndex].RowJSON
		if !json.Valid([]byte(rowJSON)) {
			return "", nil, fmt.Errorf("invalid row json")
		}
		if idx > 0 {
			b.WriteByte(',')
		}
		rowRef := buildPromptRowRef(idx + 1)
		rowRefToID[rowRef] = rows[rowIndex].RowID
		b.WriteString(`{"row_ref":`)
		b.WriteString(strconv.Quote(rowRef))
		b.WriteString(`,"row_data":`)
		b.WriteString(rowJSON)
		b.WriteByte('}')
	}
	b.WriteByte(']')
	return b.String(), rowRefToID, nil
}

func buildPromptRowRef(position int) string {
	return fmt.Sprintf("R%d", position)
}

func resolveChatResponse(raw string, rowRefToID map[string]int) (string, *int) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}

	structured, ok := parseStructuredChatResponse(raw)
	if !ok {
		return raw, nil
	}

	answer := strings.TrimSpace(structured.Answer)
	if answer == "" {
		answer = raw
	}

	matchedRowID := resolveMatchedRowRef(structured.MatchedRowRef, rowRefToID)
	return answer, matchedRowID
}

func parseStructuredChatResponse(raw string) (*chatStructuredResponse, bool) {
	jsonText := extractJSONObjectCandidate(raw)
	if jsonText == "" {
		return nil, false
	}

	var structured chatStructuredResponse
	if err := json.Unmarshal([]byte(jsonText), &structured); err != nil {
		return nil, false
	}

	structured.Answer = strings.TrimSpace(structured.Answer)
	if structured.Answer == "" {
		return nil, false
	}

	if structured.MatchedRowRef != nil {
		rowRef := normalizePromptRowRef(*structured.MatchedRowRef)
		if rowRef == "" {
			structured.MatchedRowRef = nil
		} else {
			structured.MatchedRowRef = &rowRef
		}
	}

	return &structured, true
}

func extractJSONObjectCandidate(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if strings.HasPrefix(raw, "```") {
		raw = strings.TrimPrefix(raw, "```json")
		raw = strings.TrimPrefix(raw, "```JSON")
		raw = strings.TrimPrefix(raw, "```")
		raw = strings.TrimSpace(raw)
		raw = strings.TrimSuffix(raw, "```")
		raw = strings.TrimSpace(raw)
	}

	if json.Valid([]byte(raw)) {
		return raw
	}

	start := strings.IndexByte(raw, '{')
	end := strings.LastIndexByte(raw, '}')
	if start == -1 || end == -1 || end < start {
		return ""
	}

	candidate := strings.TrimSpace(raw[start : end+1])
	if !json.Valid([]byte(candidate)) {
		return ""
	}

	return candidate
}

func resolveMatchedRowRef(rowRef *string, rowRefToID map[string]int) *int {
	if rowRef == nil {
		return nil
	}

	normalized := normalizePromptRowRef(*rowRef)
	if normalized == "" {
		return nil
	}

	rowID, ok := rowRefToID[normalized]
	if !ok {
		return nil
	}

	resolved := rowID
	return &resolved
}

func normalizePromptRowRef(rowRef string) string {
	rowRef = strings.ToUpper(strings.TrimSpace(rowRef))
	if rowRef == "" {
		return ""
	}
	if !strings.HasPrefix(rowRef, "R") {
		return ""
	}

	for _, r := range rowRef[1:] {
		if !unicode.IsDigit(r) {
			return ""
		}
	}

	return rowRef
}
