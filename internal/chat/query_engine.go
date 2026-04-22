package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"regexp"
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
)

const chatSystemInstruction = `
You are a helpful assistant answering a community data question for a non-technical user.

Style requirements:
- Answer like a human: natural, warm, and conversational.
- Prefer short paragraphs over bullet points.
- Do NOT use bullet points unless the user explicitly asks for a list.
- Do NOT sound robotic or overly formal.
- Do NOT mention JSON, database, columns, file name, file version, or any technical details.
- If the answer is not present in the provided data, say so clearly and ask 1 short follow-up question if needed.

Accuracy requirements:
- Use ONLY the provided data. Do not use outside knowledge.
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

Output requirements:
- Return ONLY the final answer text.
- Do NOT return JSON.
- Do NOT prefix the answer with labels like "answer:" or "response:".
- If clarification is needed, ask only 1 short follow-up question.
`

var (
	answerLabelRegex = regexp.MustCompile(`(?is)^\s*(?:answer|response)\s*[:\-]\s*`)
	answerFieldRegex = regexp.MustCompile(`(?is)"answer"\s*:\s*"((?:\\.|[^"\\])*)`)
)

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
	CacheKey  string
	CacheBody string
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
	answerText, err := cs.generateChatAnswer(ctx, prepared, question, audioBytes, audioMime, session)
	if err != nil {
		return nil, err
	}

	result := finalizeChatAnswer(question, questionNorm, session, answerText)
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

	promptJSON, err := buildPromptJSONArray(dataset.rows, selectedIndexes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file data: %w", err)
	}

	cacheBody := buildChatCacheBody(dataset, communities, len(selectedIndexes), promptJSON)
	return &preparedChatDataset{
		CacheKey:  fmt.Sprintf("%d:%d:%d:%s", dataset.FileID, dataset.Version, dataset.ConfigAt.UnixNano(), chatCommunitiesCacheKey(communities)),
		CacheBody: cacheBody,
	}, nil
}

func buildChatCacheBody(dataset *chatDatasetCacheEntry, communities []string, rowCount int, promptJSON string) string {
	_ = dataset
	_ = communities
	_ = rowCount
	return "DATA (only source of truth):\n" + promptJSON
}

func buildPromptJSONArray(rows []cachedChatRow, indexes []int) (string, error) {
	if len(indexes) == 0 {
		return "[]", nil
	}

	var b strings.Builder
	b.WriteByte('[')
	for idx, rowIndex := range indexes {
		rowJSON := rows[rowIndex].RowJSON
		if !json.Valid([]byte(rowJSON)) {
			return "", fmt.Errorf("invalid row json")
		}
		if idx > 0 {
			b.WriteByte(',')
		}
		rowRef := buildPromptRowRef(idx + 1)
		b.WriteString(`{"row_ref":`)
		b.WriteString(strconv.Quote(rowRef))
		b.WriteString(`,"row_data":`)
		b.WriteString(rowJSON)
		b.WriteByte('}')
	}
	b.WriteByte(']')
	return b.String(), nil
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

func (cs *ChatService) generateChatAnswer(
	ctx context.Context,
	prepared *preparedChatDataset,
	question string,
	audioBytes []byte,
	audioMime string,
	session *chatSessionState,
) (string, error) {
	requestPrompt := buildChatTurnPrompt(question, session, len(audioBytes) > 0)
	raw, _, err := cs.generateChatWithModel(ctx, chatPrimaryModel, prepared, requestPrompt, audioBytes, audioMime)
	if err == nil {
		answer := sanitizeAnswerText(raw)
		if answer != "" {
			return answer, nil
		}
		return "", fmt.Errorf("no response from Gemini")
	}
	if !isRateLimit429(err) {
		return "", err
	}

	raw, _, err = cs.generateChatWithModel(ctx, chatFallbackModel, prepared, requestPrompt, audioBytes, audioMime)
	if err != nil {
		return "", err
	}
	answer := sanitizeAnswerText(raw)
	if answer == "" {
		return "", fmt.Errorf("no response from Gemini")
	}
	return answer, nil
}

func (cs *ChatService) generateChatWithModel(
	ctx context.Context,
	model string,
	prepared *preparedChatDataset,
	requestPrompt string,
	audioBytes []byte,
	audioMime string,
) (string, *genai.GenerateContentResponse, error) {
	directPrompt := buildDirectChatPrompt(prepared, requestPrompt)
	return cs.generateChatRequest(ctx, model, directPrompt, audioBytes, audioMime, newChatGenerateConfig())
}

func buildDirectChatPrompt(prepared *preparedChatDataset, requestPrompt string) string {
	return strings.TrimSpace(chatSystemInstruction) + "\n\n" + prepared.CacheBody + "\n\n" + requestPrompt
}

func (cs *ChatService) generateChatRequest(
	ctx context.Context,
	model string,
	prompt string,
	audioBytes []byte,
	audioMime string,
	config *genai.GenerateContentConfig,
) (string, *genai.GenerateContentResponse, error) {
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
		return "", nil, err
	}

	out := strings.TrimSpace(firstTextFromResp(resp))
	if out == "" {
		return "", resp, fmt.Errorf("no response from Gemini")
	}
	return out, resp, nil
}

func buildChatTurnPrompt(question string, session *chatSessionState, hasAudio bool) string {
	question = strings.TrimSpace(question)
	questionNorm := normalizeSearchText(question)

	var b strings.Builder
	b.WriteString("Answer using only the provided data.\n")
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

func finalizeChatAnswer(question string, questionNorm string, session *chatSessionState, answer string) *ChatResult {
	answer = sanitizeAnswerText(answer)
	needsClarification := isClarificationQuestion(answer)

	if needsClarification {
		clarificationQuestion := answer
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
	return &ChatResult{Answer: answer}
}

func samePrompt(a, b string) bool {
	return normalizeSearchText(a) == normalizeSearchText(b)
}

func sanitizeAnswerText(answer string) string {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return ""
	}

	answer = strings.Trim(answer, "`")
	if unwrapped, ok := unquoteJSONString(answer); ok {
		answer = strings.TrimSpace(unwrapped)
	}

	if jsonText := extractJSONObjectCandidate(answer); jsonText != "" {
		var payload struct {
			Answer string `json:"answer"`
		}
		if err := json.Unmarshal([]byte(jsonText), &payload); err == nil {
			if extracted := strings.TrimSpace(payload.Answer); extracted != "" {
				answer = extracted
			}
		}
	}

	if matches := answerFieldRegex.FindStringSubmatch(answer); len(matches) == 2 {
		if extracted := strings.TrimSpace(decodeJSONStringLiteral(matches[1])); extracted != "" {
			answer = extracted
		}
	} else if extracted, ok := extractPartialAnswerField(answer); ok {
		answer = extracted
	}

	answer = answerLabelRegex.ReplaceAllString(answer, "")
	answer = strings.Trim(answer, "\"")
	return strings.TrimSpace(answer)
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
		if strings.HasPrefix(raw, "{") {
			return raw
		}
		var wrapped string
		if err := json.Unmarshal([]byte(raw), &wrapped); err == nil {
			return extractJSONObjectCandidate(wrapped)
		}
		return ""
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

func unquoteJSONString(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 2 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return "", false
	}

	var unwrapped string
	if err := json.Unmarshal([]byte(raw), &unwrapped); err != nil {
		return "", false
	}
	return strings.TrimSpace(unwrapped), true
}

func decodeJSONStringLiteral(raw string) string {
	var decoded string
	if err := json.Unmarshal([]byte(`"`+raw+`"`), &decoded); err == nil {
		return decoded
	}
	return raw
}

func extractPartialAnswerField(raw string) (string, bool) {
	answerMarkers := []string{`"answer"`, `\"answer\"`}
	for _, marker := range answerMarkers {
		idx := strings.Index(raw, marker)
		if idx == -1 {
			continue
		}

		rest := raw[idx+len(marker):]
		colonIdx := strings.Index(rest, ":")
		if colonIdx == -1 {
			continue
		}

		rest = strings.TrimSpace(rest[colonIdx+1:])
		if rest == "" {
			continue
		}

		escaped := false
		if strings.HasPrefix(rest, `\"`) {
			escaped = true
			rest = rest[2:]
		} else if strings.HasPrefix(rest, `"`) {
			rest = rest[1:]
		} else {
			continue
		}

		var b strings.Builder
		for i := 0; i < len(rest); i++ {
			if escaped {
				if i+1 < len(rest) && rest[i] == '\\' && rest[i+1] == '"' {
					break
				}
				b.WriteByte(rest[i])
				continue
			}

			if rest[i] == '"' && (i == 0 || rest[i-1] != '\\') {
				break
			}
			b.WriteByte(rest[i])
		}

		value := decodePartialJSONStringLiteral(b.String())
		value = strings.TrimSpace(value)
		if value != "" {
			return value, true
		}
	}
	return "", false
}

func decodePartialJSONStringLiteral(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	if decoded := decodeJSONStringLiteral(raw); decoded != raw {
		return decoded
	}

	replacer := strings.NewReplacer(
		`\\n`, "\n",
		`\\r`, "\r",
		`\\t`, "\t",
		`\"`, `"`,
		`\\`, `\`,
	)
	return replacer.Replace(raw)
}

func newChatGenerateConfig() *genai.GenerateContentConfig {
	return &genai.GenerateContentConfig{
		ResponseMIMEType: "text/plain",
		MaxOutputTokens:  1536,
		Temperature:      float32Ptr(0),
	}
}

func isClarificationQuestion(answer string) bool {
	answer = strings.TrimSpace(answer)
	if answer == "" || !strings.HasSuffix(answer, "?") {
		return false
	}

	lower := strings.ToLower(answer)
	for _, prefix := range []string{
		"which ",
		"who ",
		"what ",
		"when ",
		"where ",
		"do you mean",
		"did you mean",
		"could you clarify",
		"can you clarify",
		"are you asking",
		"would you like",
		"do you want",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

func float32Ptr(v float32) *float32 {
	return &v
}
