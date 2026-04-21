package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	dc "nordik-drive-api/internal/dataconfig"
	f "nordik-drive-api/internal/file"

	"google.golang.org/genai"
	"gorm.io/gorm"
)

const (
	chatPrimaryModel  = "gemini-2.5-pro"
	chatFallbackModel = "gemini-2.5-flash"
	chatContextTTL    = 24 * time.Hour
)

const chatCachedSystemInstruction = `
You are a helpful assistant answering a community data question for a non-technical user.

Style requirements:
- Answer like a human: natural, warm, and conversational.
- Prefer short paragraphs over bullet points.
- Do NOT use bullet points unless the user explicitly asks for a list.
- Do NOT sound robotic or overly formal.
- Do NOT mention JSON, database, columns, file name, file version, cached content, or any technical details.
- If the answer is not present in the provided data, say so clearly and ask 1 short follow-up question only when absolutely needed.
- Maintain an empathetic and respectful tone, especially when discussing deaths or sensitive topics.

Accuracy requirements:
- Use ONLY the cached file context and the latest user turn. Do not use outside knowledge.
- Carefully analyze ALL relevant records before answering.
- Do not stop after finding the first match.
- Always check if multiple equally correct answers exist.
- Never return only one answer if multiple valid answers are present.
- For questions involving highest, lowest, most, least, first, last, top, or similar comparisons:
  verify that no other entries share the same value before answering.

Session rules:
- Treat prior conversation context as optional support only.
- If the latest user turn is a fresh standalone question, ignore prior conversation context.
- Use prior conversation context only when the latest turn clearly refers back or is explicitly answering a clarification question.
- Never let older conversation context change the answer to a new standalone question.

Clarification rules:
- Ask a clarification question only when it is necessary to avoid a wrong answer.
- Keep clarification questions short and specific.
- If the prompt says no clarification budget remains, do not ask another clarification question. Instead say the records do not allow an exact answer.

Output requirements:
Return ONLY a single JSON object with this shape:
{
  "answer": "your natural-language answer",
  "matched_row_ref": "R1" or null,
  "needs_clarification": false,
  "clarification_question": null
}

Rules for matched_row_ref:
- Set it only when the user's question is about a person and exactly one row is the best match.
- You may also set it when your answer is based entirely on one single row, even if the question is not explicitly about a person.
- If the answer depends on multiple rows, or more than one row could match, set matched_row_ref to null.
- If a person's name appears misspelled but the intended single row is still clear from the data, you may still choose that row_ref.
- Use only row_ref values that appear in the DATA.
- Never invent, guess, or approximate a row_ref.

Rules for clarification:
- Set "needs_clarification" to true only when clarification is necessary.
- When "needs_clarification" is true, set "clarification_question" to the exact short question to ask.
- When "needs_clarification" is false, set "clarification_question" to null or an empty string.
`

type chatDatasetCacheEntry struct {
	FileID           uint
	Filename         string
	Version          int
	ColumnsOrderJSON string
	ConfigJSON       string
	ConfigAt         time.Time
	rows             []cachedChatRow
	prepared         sync.Map
}

type cachedChatRow struct {
	RowID     int
	RowJSON   string
	Community string
}

type preparedChatDataset struct {
	CacheKey   string
	CacheBody  string
	RowRefToID map[string]int
}

type chatContextCacheEntry struct {
	Name      string
	Model     string
	ExpiresAt time.Time
}

type chatStructuredResponse struct {
	Answer                string  `json:"answer"`
	MatchedRowRef         *string `json:"matched_row_ref"`
	NeedsClarification    bool    `json:"needs_clarification"`
	ClarificationQuestion string  `json:"clarification_question"`
}

func (cs *ChatService) ChatForUser(userID int64, question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	if cs.DB == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	if cs.Client == nil {
		return nil, fmt.Errorf("genai client not initialized")
	}

	file, configRecord, err := cs.loadChatFileMetadata(filename)
	if err != nil {
		return nil, err
	}

	filteredCommunities := normalizeCommunities(communities)
	dataset, err := cs.getOrLoadChatDataset(file, configRecord)
	if err != nil {
		return nil, err
	}

	prepared, err := dataset.preparedForCommunities(filteredCommunities)
	if err != nil {
		return nil, err
	}

	audioBytes, audioMime, err := loadOptionalAudio(audioFile)
	if err != nil {
		return nil, err
	}

	question = strings.TrimSpace(question)
	if question == "" && len(audioBytes) == 0 {
		return nil, fmt.Errorf("question or audio is required")
	}

	session := cs.loadSession(userID, file.ID, file.Filename, file.Version, filteredCommunities)
	questionNorm := normalizeSearchText(question)
	if session.Pending != nil && !isLikelyClarificationReply(questionNorm) && !looksLikeFollowUpQuestion(questionNorm) {
		session.Pending = nil
	}

	ctx := context.Background()
	structured, err := cs.generateChatStructuredResponse(ctx, prepared, question, audioBytes, audioMime, session)
	if err != nil {
		return nil, err
	}

	result := finalizeChatStructuredResponse(question, questionNorm, prepared, session, structured)
	cs.saveSession(userID, session)
	return result, nil
}

func (cs *ChatService) loadChatFileMetadata(filename string) (f.File, *dc.DataConfig, error) {
	var file f.File
	if err := cs.DB.Select("id, filename, version, columns_order").
		Where("filename = ?", filename).
		Order("version DESC").
		First(&file).Error; err != nil {
		return f.File{}, nil, fmt.Errorf("file not found")
	}

	var configRecord dc.DataConfig
	err := cs.DB.
		Where("is_active = ?", true).
		Where("lower(file_name) = lower(?)", file.Filename).
		Order("updated_at DESC").
		Order("id DESC").
		Take(&configRecord).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return file, nil, nil
		}
		return f.File{}, nil, err
	}

	return file, &configRecord, nil
}

func (cs *ChatService) getOrLoadChatDataset(file f.File, configRecord *dc.DataConfig) (*chatDatasetCacheEntry, error) {
	cacheKey := chatDatasetCacheKey(file.ID, file.Version, configRecord)
	if cached, ok := cs.datasetCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatDatasetCacheEntry); ok {
			return entry, nil
		}
	}

	var rawRows []f.FileData
	if err := cs.DB.Select("id, row_data").
		Where("file_id = ? AND version = ?", file.ID, file.Version).
		Order("id ASC").
		Find(&rawRows).Error; err != nil {
		return nil, fmt.Errorf("file data not found: %w", err)
	}

	rows := make([]cachedChatRow, 0, len(rawRows))
	for _, rawRow := range rawRows {
		rows = append(rows, buildCachedChatRow(rawRow))
	}

	entry := &chatDatasetCacheEntry{
		FileID:           file.ID,
		Filename:         file.Filename,
		Version:          file.Version,
		ColumnsOrderJSON: strings.TrimSpace(string(file.ColumnsOrder)),
		rows:             rows,
	}
	if configRecord != nil {
		entry.ConfigJSON = strings.TrimSpace(string(configRecord.Config))
		entry.ConfigAt = configRecord.UpdatedAt.UTC()
	}

	actual, _ := cs.datasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatDatasetCacheEntry); ok {
		return cached, nil
	}

	return entry, nil
}

func chatDatasetCacheKey(fileID uint, version int, configRecord *dc.DataConfig) string {
	configStamp := int64(0)
	if configRecord != nil {
		configStamp = configRecord.UpdatedAt.UTC().UnixNano()
	}
	return fmt.Sprintf("%d:%d:%d", fileID, version, configStamp)
}

func chatCommunitiesCacheKey(communities []string) string {
	if len(communities) == 0 {
		return "__all__"
	}
	return strings.Join(communities, "\x1f")
}

func (dataset *chatDatasetCacheEntry) preparedForCommunities(communities []string) (*preparedChatDataset, error) {
	cacheKey := chatCommunitiesCacheKey(communities)
	if cached, ok := dataset.prepared.Load(cacheKey); ok {
		if prepared, ok := cached.(*preparedChatDataset); ok {
			return prepared, nil
		}
	}

	prepared, err := buildPreparedChatDataset(dataset, communities)
	if err != nil {
		return nil, err
	}

	actual, _ := dataset.prepared.LoadOrStore(cacheKey, prepared)
	if cached, ok := actual.(*preparedChatDataset); ok {
		return cached, nil
	}

	return prepared, nil
}

func buildPreparedChatDataset(dataset *chatDatasetCacheEntry, communities []string) (*preparedChatDataset, error) {
	selectedIndexes := make([]int, 0, len(dataset.rows))
	if len(communities) == 0 {
		for idx := range dataset.rows {
			selectedIndexes = append(selectedIndexes, idx)
		}
	} else {
		allowed := make(map[string]struct{}, len(communities))
		for _, community := range communities {
			allowed[community] = struct{}{}
		}
		for idx, row := range dataset.rows {
			if _, ok := allowed[row.Community]; ok {
				selectedIndexes = append(selectedIndexes, idx)
			}
		}
	}

	promptJSON, rowRefToID, err := buildPromptJSONArray(dataset.rows, selectedIndexes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file data: %w", err)
	}

	cacheBody := buildChatCacheBody(dataset, communities, len(selectedIndexes), promptJSON)
	return &preparedChatDataset{
		CacheKey:   fmt.Sprintf("%d:%d:%d:%s", dataset.FileID, dataset.Version, dataset.ConfigAt.UnixNano(), chatCommunitiesCacheKey(communities)),
		CacheBody:  cacheBody,
		RowRefToID: rowRefToID,
	}, nil
}

func buildChatCacheBody(dataset *chatDatasetCacheEntry, communities []string, rowCount int, promptJSON string) string {
	sections := []string{
		"FILE CONTEXT",
		fmt.Sprintf("Filename: %s", dataset.Filename),
		fmt.Sprintf("Version: %d", dataset.Version),
		fmt.Sprintf("Rows in scope: %d", rowCount),
		fmt.Sprintf("Community scope: %s", renderCommunityScope(communities)),
	}

	if strings.TrimSpace(dataset.ColumnsOrderJSON) != "" {
		sections = append(sections, "COLUMNS ORDER:\n"+dataset.ColumnsOrderJSON)
	}
	if strings.TrimSpace(dataset.ConfigJSON) != "" {
		sections = append(sections, "ACTIVE CONFIG:\n"+dataset.ConfigJSON)
	}
	sections = append(sections, "DATA (only source of truth):\n"+promptJSON)
	return strings.Join(sections, "\n\n")
}

func renderCommunityScope(communities []string) string {
	if len(communities) == 0 {
		return "All communities"
	}
	return strings.Join(communities, ", ")
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

func loadOptionalAudio(audioFile *multipart.FileHeader) ([]byte, string, error) {
	if audioFile == nil {
		return nil, "", nil
	}

	fh, err := openMultipartFileHook(audioFile)
	if err != nil {
		return nil, "", fmt.Errorf("failed to open audio file: %w", err)
	}
	defer fh.Close()

	audioBytes, err := readAllHook(fh)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read audio file: %w", err)
	}

	return audioBytes, audioFile.Header.Get("Content-Type"), nil
}

func (cs *ChatService) generateChatStructuredResponse(
	ctx context.Context,
	prepared *preparedChatDataset,
	question string,
	audioBytes []byte,
	audioMime string,
	session *chatSessionState,
) (*chatStructuredResponse, error) {
	requestPrompt := buildChatTurnPrompt(question, session, len(audioBytes) > 0)

	raw, err := cs.generateChatWithModel(ctx, chatPrimaryModel, prepared, requestPrompt, audioBytes, audioMime)
	if err != nil && isRateLimit429(err) {
		raw, err = cs.generateChatWithModel(ctx, chatFallbackModel, prepared, requestPrompt, audioBytes, audioMime)
	}
	if err != nil {
		return nil, err
	}

	structured, ok := parseStructuredChatResponse(raw)
	if !ok {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return nil, fmt.Errorf("no response from Gemini")
		}
		return &chatStructuredResponse{Answer: raw}, nil
	}
	return structured, nil
}

func (cs *ChatService) generateChatWithModel(
	ctx context.Context,
	model string,
	prepared *preparedChatDataset,
	requestPrompt string,
	audioBytes []byte,
	audioMime string,
) (string, error) {
	cacheEntry, cacheErr := cs.ensureChatContextCache(ctx, model, prepared)
	if cacheErr == nil {
		raw, err := cs.generateChatRequest(ctx, model, requestPrompt, audioBytes, audioMime, &genai.GenerateContentConfig{
			CachedContent:    cacheEntry.Name,
			ResponseMIMEType: "application/json",
			MaxOutputTokens:  1024,
			Temperature:      float32Ptr(0.1),
		})
		if err == nil {
			return raw, nil
		}
		if isRateLimit429(err) {
			return "", err
		}
		if isMissingCachedContentError(err) {
			cs.contextCache.Delete(chatContextCacheKey(model, prepared.CacheKey))
		}
	}

	directPrompt := buildDirectChatPrompt(prepared, requestPrompt)
	return cs.generateChatRequest(ctx, model, directPrompt, audioBytes, audioMime, &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
		MaxOutputTokens:  1024,
		Temperature:      float32Ptr(0.1),
	})
}

func buildDirectChatPrompt(prepared *preparedChatDataset, requestPrompt string) string {
	return strings.TrimSpace(chatCachedSystemInstruction) + "\n\n" + prepared.CacheBody + "\n\n" + requestPrompt
}

func (cs *ChatService) generateChatRequest(
	ctx context.Context,
	model string,
	prompt string,
	audioBytes []byte,
	audioMime string,
	config *genai.GenerateContentConfig,
) (string, error) {
	parts := []*genai.Part{{Text: prompt}}
	if len(audioBytes) > 0 {
		audioMime = strings.TrimSpace(audioMime)
		if audioMime == "" || audioMime == "application/octet-stream" {
			audioMime = "audio/webm"
		}
		parts = append(parts, &genai.Part{
			InlineData: &genai.Blob{Data: audioBytes, MIMEType: audioMime},
		})
	}

	resp, err := genaiGenerateContentHook(cs.Client, ctx, model, []*genai.Content{
		{Role: "user", Parts: parts},
	}, config)
	if err != nil {
		return "", err
	}

	out := strings.TrimSpace(firstTextFromResp(resp))
	if out == "" {
		return "", fmt.Errorf("no response from Gemini")
	}
	return out, nil
}

func (cs *ChatService) ensureChatContextCache(ctx context.Context, model string, prepared *preparedChatDataset) (*chatContextCacheEntry, error) {
	cacheKey := chatContextCacheKey(model, prepared.CacheKey)
	if cached, ok := cs.contextCache.Load(cacheKey); ok {
		if entry, ok := cached.(*chatContextCacheEntry); ok {
			if time.Now().UTC().Before(entry.ExpiresAt) {
				return entry, nil
			}
			cs.contextCache.Delete(cacheKey)
		}
	}

	config := &genai.CreateCachedContentConfig{
		TTL:         chatContextTTL,
		DisplayName: buildContextCacheDisplayName(model, prepared.CacheKey),
		Contents: []*genai.Content{
			{
				Role: "user",
				Parts: []*genai.Part{
					{Text: prepared.CacheBody},
				},
			},
		},
		SystemInstruction: &genai.Content{
			Parts: []*genai.Part{
				{Text: strings.TrimSpace(chatCachedSystemInstruction)},
			},
		},
	}

	cachedContent, err := genaiCreateCachedContentHook(cs.Client, ctx, model, config)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cachedContent.Name) == "" {
		return nil, fmt.Errorf("cached content created without a name")
	}

	expiresAt := cachedContent.ExpireTime.UTC()
	if expiresAt.IsZero() {
		expiresAt = time.Now().UTC().Add(chatContextTTL)
	}

	entry := &chatContextCacheEntry{
		Name:      cachedContent.Name,
		Model:     model,
		ExpiresAt: expiresAt,
	}
	cs.contextCache.Store(cacheKey, entry)
	return entry, nil
}

func chatContextCacheKey(model, preparedKey string) string {
	return model + "|" + preparedKey
}

func buildContextCacheDisplayName(model, preparedKey string) string {
	safe := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		}
		return '-'
	}, strings.ToLower(model+"-"+preparedKey))
	if len(safe) > 120 {
		safe = safe[:120]
	}
	return strings.Trim(safe, "-")
}

func buildChatTurnPrompt(question string, session *chatSessionState, hasAudio bool) string {
	question = strings.TrimSpace(question)
	questionNorm := normalizeSearchText(question)

	var b strings.Builder
	b.WriteString("Answer using only the cached file context.\n")
	if hasAudio {
		b.WriteString("If audio is attached, use it only to understand the latest user question.\n")
	}

	if session != nil && session.Pending != nil && isLikelyClarificationReply(questionNorm) {
		remaining := chatClarificationBudget - session.Pending.Attempts
		if remaining < 0 {
			remaining = 0
		}
		b.WriteString("The latest user turn is a reply to a clarification.\n")
		if strings.TrimSpace(session.Pending.OriginalQuestion) != "" {
			b.WriteString("Original question:\n")
			b.WriteString(strings.TrimSpace(session.Pending.OriginalQuestion))
			b.WriteString("\n")
		}
		if strings.TrimSpace(session.Pending.Prompt) != "" {
			b.WriteString("Previous clarification question:\n")
			b.WriteString(strings.TrimSpace(session.Pending.Prompt))
			b.WriteString("\n")
		}
		b.WriteString("User reply:\n")
		if question != "" {
			b.WriteString(question)
		} else {
			b.WriteString("[No typed question text provided. Use the attached audio if present.]")
		}
		b.WriteString("\n")
		b.WriteString(fmt.Sprintf("Clarification budget remaining after this turn: %d\n", remaining))
		if remaining <= 0 {
			b.WriteString("Do not ask another clarification question. If still unsure, say the records do not allow an exact answer.\n")
		}
		return b.String()
	}

	if shouldUseSessionContext(questionNorm, session) {
		b.WriteString("This latest question appears to be a follow-up. Use the conversation summary only if it is needed to resolve explicit references.\n")
		b.WriteString(session.summaryForPrompt())
		b.WriteString("\n")
	} else {
		b.WriteString("This latest question stands on its own. Ignore prior conversation context unless the question explicitly refers back.\n")
	}

	b.WriteString(fmt.Sprintf("Clarification budget available for this question path: %d\n", chatClarificationBudget))
	b.WriteString("Latest user question:\n")
	if question != "" {
		b.WriteString(question)
	} else {
		b.WriteString("[No typed question text provided. Use the attached audio if present.]")
	}
	b.WriteString("\n")

	return b.String()
}

func shouldUseSessionContext(questionNorm string, session *chatSessionState) bool {
	if session == nil || len(session.RecentTurns) == 0 {
		return false
	}
	if looksLikeFollowUpQuestion(questionNorm) {
		return true
	}
	if session.Pending != nil && isLikelyClarificationReply(questionNorm) {
		return true
	}
	return false
}

func looksLikeFollowUpQuestion(questionNorm string) bool {
	if questionNorm == "" {
		return false
	}
	prefixes := []string{
		"what about",
		"how about",
		"and what about",
		"and how about",
		"same person",
		"same one",
		"same child",
		"same student",
		"that person",
		"that child",
		"that student",
		"those children",
		"those students",
		"their names",
		"their communities",
		"their community",
		"their causes",
		"tell me more",
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(questionNorm, prefix) {
			return true
		}
	}

	words := strings.Fields(questionNorm)
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "he", "she", "they", "them", "their", "his", "her", "those", "that":
		return true
	}
	return false
}

func isLikelyClarificationReply(questionNorm string) bool {
	if questionNorm == "" {
		return false
	}

	words := strings.Fields(questionNorm)
	if len(words) == 0 || len(words) > 6 {
		return false
	}

	switch words[0] {
	case "what", "when", "where", "who", "why", "how", "did", "does", "do", "is", "are", "was", "were", "can", "could", "would", "should":
		return false
	}
	return true
}

func normalizeSearchText(text string) string {
	text = strings.TrimSpace(strings.ToLower(text))
	if text == "" {
		return ""
	}

	var b strings.Builder
	lastSpace := false
	for _, r := range text {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}

	return strings.Join(strings.Fields(b.String()), " ")
}

func finalizeChatStructuredResponse(
	question string,
	questionNorm string,
	prepared *preparedChatDataset,
	session *chatSessionState,
	structured *chatStructuredResponse,
) *ChatResult {
	answer := strings.TrimSpace(structured.Answer)
	needsClarification := structured.NeedsClarification || strings.TrimSpace(structured.ClarificationQuestion) != ""

	if needsClarification {
		clarificationQuestion := strings.TrimSpace(structured.ClarificationQuestion)
		if clarificationQuestion == "" {
			clarificationQuestion = answer
		}
		if clarificationQuestion == "" {
			clarificationQuestion = "Could you clarify that a little more?"
		}

		isReply := session != nil && session.Pending != nil && isLikelyClarificationReply(questionNorm)
		if isReply && samePrompt(session.Pending.Prompt, clarificationQuestion) {
			answer = "I can't determine that exactly from the available data."
			session.Pending = nil
			session.registerTurn(question, answer)
			return &ChatResult{Answer: answer}
		}

		attempts := 1
		originalQuestion := question
		if isReply {
			attempts = session.Pending.Attempts + 1
			if strings.TrimSpace(session.Pending.OriginalQuestion) != "" {
				originalQuestion = session.Pending.OriginalQuestion
			}
		}

		if attempts > chatClarificationBudget {
			answer = "I can't determine that exactly from the available data."
			session.Pending = nil
			session.registerTurn(question, answer)
			return &ChatResult{Answer: answer}
		}

		answer = clarificationQuestion
		session.Pending = &chatPendingClarification{
			Prompt:           clarificationQuestion,
			Attempts:         attempts,
			OriginalQuestion: originalQuestion,
		}
		session.registerTurn(question, answer)
		return &ChatResult{Answer: answer}
	}

	session.Pending = nil
	if answer == "" {
		answer = "I couldn't find an answer in the available data."
	}
	session.registerTurn(question, answer)
	return &ChatResult{
		Answer:       answer,
		MatchedRowID: resolveMatchedRowRef(structured.MatchedRowRef, prepared.RowRefToID),
	}
}

func samePrompt(a, b string) bool {
	return normalizeSearchText(a) == normalizeSearchText(b)
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
	structured.ClarificationQuestion = strings.TrimSpace(structured.ClarificationQuestion)
	if structured.MatchedRowRef != nil {
		rowRef := normalizePromptRowRef(*structured.MatchedRowRef)
		if rowRef == "" {
			structured.MatchedRowRef = nil
		} else {
			structured.MatchedRowRef = &rowRef
		}
	}

	if structured.Answer == "" && !structured.NeedsClarification && structured.ClarificationQuestion == "" {
		return nil, false
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

func isMissingCachedContentError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "cached") &&
		(strings.Contains(msg, "not found") ||
			strings.Contains(msg, "404") ||
			strings.Contains(msg, "expired"))
}

func float32Ptr(v float32) *float32 {
	return &v
}
