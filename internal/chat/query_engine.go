package chat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"regexp"
	"sort"
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
	RowID      int
	RowJSON    string
	Community  string
	Values     map[string]string
	Normalized map[string]string
}

type preparedChatDataset struct {
	CacheKey  string
	CacheBody string
}

type chatContextCacheEntry struct {
	Name      string
	Model     string
	ExpiresAt time.Time
}

type verifiedDeathYearAggregate struct {
	ScopeOriginal   string
	ScopeNormalized string
	Years           []int
	Count           int
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
	if session.Pending == nil && !looksLikeFollowUpQuestion(questionNorm) && !isLikelyClarificationReply(questionNorm) {
		if deterministicAnswer, handled, err := cs.tryDeterministicChatAnswer(ctx, dataset, filteredCommunities, question, questionNorm); err != nil {
			return nil, err
		} else if handled {
			result := finalizeChatAnswer(question, questionNorm, session, deterministicAnswer)
			cs.saveSession(userID, session)
			return result, nil
		}
	}

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

func renderCommunityScope(communities []string) string {
	if len(communities) == 0 {
		return "All communities"
	}
	return strings.Join(communities, ", ")
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
		RowID:      int(rawRow.ID),
		RowJSON:    string(rawRow.RowData),
		Values:     make(map[string]string),
		Normalized: make(map[string]string),
	}

	var rowMap map[string]interface{}
	if err := json.Unmarshal(rawRow.RowData, &rowMap); err != nil {
		return row
	}
	for key, rawValue := range rowMap {
		stringValue := stringifyChatValue(rawValue)
		row.Values[key] = stringValue
		row.Normalized[key] = normalizeSearchText(stringValue)
	}
	row.Community = extractCommunityValue(rowMap)
	return row
}

func stringifyChatValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	case float64:
		return strings.TrimSpace(strconv.FormatFloat(typed, 'f', -1, 64))
	case float32:
		return strings.TrimSpace(strconv.FormatFloat(float64(typed), 'f', -1, 32))
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case uint:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
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

func (cs *ChatService) tryDeterministicChatAnswer(
	ctx context.Context,
	dataset *chatDatasetCacheEntry,
	communities []string,
	question string,
	questionNorm string,
) (string, bool, error) {
	aggregate, ok := detectHighestDeathsByYearAggregate(dataset, communities, question, questionNorm)
	if !ok {
		return "", false, nil
	}

	answer, err := cs.renderVerifiedDeathYearAggregate(ctx, question, aggregate)
	if err != nil {
		return renderVerifiedDeathYearAggregateFallback(aggregate), true, nil
	}
	return answer, true, nil
}

func detectHighestDeathsByYearAggregate(
	dataset *chatDatasetCacheEntry,
	communities []string,
	question string,
	questionNorm string,
) (*verifiedDeathYearAggregate, bool) {
	if !isHighestDeathsByYearQuestion(questionNorm) {
		return nil, false
	}

	scopeOriginal, scopeNormalized := extractAggregateScope(question, questionNorm)
	yearCounts := make(map[int]int)

	for _, row := range dataset.rows {
		if !rowAllowedByCommunities(row, communities) {
			continue
		}
		if scopeNormalized != "" && !rowMatchesAggregateScope(row, scopeNormalized) {
			continue
		}
		year, ok := extractDeathYearFromRow(row)
		if !ok {
			continue
		}
		yearCounts[year]++
	}

	if len(yearCounts) == 0 {
		return nil, false
	}

	maxCount := 0
	years := make([]int, 0, len(yearCounts))
	for year, count := range yearCounts {
		switch {
		case count > maxCount:
			maxCount = count
			years = []int{year}
		case count == maxCount:
			years = append(years, year)
		}
	}
	if maxCount == 0 || len(years) == 0 {
		return nil, false
	}
	sort.Ints(years)

	return &verifiedDeathYearAggregate{
		ScopeOriginal:   scopeOriginal,
		ScopeNormalized: scopeNormalized,
		Years:           years,
		Count:           maxCount,
	}, true
}

func isHighestDeathsByYearQuestion(questionNorm string) bool {
	if questionNorm == "" {
		return false
	}
	if !strings.Contains(questionNorm, "death") || !strings.Contains(questionNorm, "year") {
		return false
	}
	if strings.Contains(questionNorm, "highest number of deaths") {
		return true
	}
	if strings.Contains(questionNorm, "most deaths") {
		return true
	}
	if strings.Contains(questionNorm, "highest deaths") {
		return true
	}
	return false
}

var aggregateScopeSuffixRe = regexp.MustCompile(`(?i)\boccur(?:red)?\s+(?:at|in)\s+(.+?)\s*\??$`)

func extractAggregateScope(question string, questionNorm string) (string, string) {
	match := aggregateScopeSuffixRe.FindStringSubmatch(strings.TrimSpace(question))
	if len(match) == 2 {
		scopeOriginal := strings.TrimSpace(strings.Trim(match[1], " ?."))
		return scopeOriginal, normalizeAggregateScope(scopeOriginal)
	}

	match = aggregateScopeSuffixRe.FindStringSubmatch(strings.TrimSpace(questionNorm))
	if len(match) == 2 {
		scopeNorm := normalizeAggregateScope(match[1])
		return strings.TrimSpace(match[1]), scopeNorm
	}
	return "", ""
}

func normalizeAggregateScope(scope string) string {
	scope = normalizeSearchText(scope)
	if scope == "" {
		return ""
	}
	stopwords := map[string]struct{}{
		"the": {}, "a": {}, "an": {}, "school": {}, "residential": {}, "indian": {}, "institution": {},
		"irs": {}, "at": {}, "in": {}, "of": {}, "for": {},
	}
	parts := make([]string, 0, len(strings.Fields(scope)))
	for _, part := range strings.Fields(scope) {
		if _, skip := stopwords[part]; skip {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 {
		return scope
	}
	return strings.Join(parts, " ")
}

func rowAllowedByCommunities(row cachedChatRow, communities []string) bool {
	if len(communities) == 0 {
		return true
	}
	for _, community := range communities {
		if row.Community == community {
			return true
		}
	}
	return false
}

func rowMatchesAggregateScope(row cachedChatRow, scopeNormalized string) bool {
	if scopeNormalized == "" {
		return true
	}
	for fieldName, valueNorm := range row.Normalized {
		if valueNorm == "" {
			continue
		}
		fieldNorm := normalizeSearchText(fieldName)
		if !isAggregateScopeField(fieldNorm) {
			continue
		}
		if aggregateScopeMatchesValue(scopeNormalized, valueNorm) {
			return true
		}
	}
	return false
}

func isAggregateScopeField(fieldNorm string) bool {
	return strings.Contains(fieldNorm, "school") ||
		strings.Contains(fieldNorm, "residential") ||
		strings.Contains(fieldNorm, "institution") ||
		strings.Contains(fieldNorm, "place") ||
		strings.Contains(fieldNorm, "location") ||
		strings.Contains(fieldNorm, "community") ||
		strings.Contains(fieldNorm, "reserve") ||
		strings.Contains(fieldNorm, "first nation")
}

func aggregateScopeMatchesValue(scopeNormalized, valueNormalized string) bool {
	if scopeNormalized == "" || valueNormalized == "" {
		return false
	}
	if strings.Contains(valueNormalized, scopeNormalized) {
		return true
	}
	for _, token := range strings.Fields(scopeNormalized) {
		if token == "" || !strings.Contains(valueNormalized, token) {
			return false
		}
	}
	return true
}

func extractDeathYearFromRow(row cachedChatRow) (int, bool) {
	type candidate struct {
		score int
		value string
	}
	candidates := make([]candidate, 0, len(row.Values))
	for fieldName, value := range row.Values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		score := deathDateFieldScore(normalizeSearchText(fieldName))
		if score <= 0 {
			continue
		}
		candidates = append(candidates, candidate{score: score, value: value})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	for _, candidate := range candidates {
		if year, ok := extractSingleYear(candidate.value); ok {
			return year, true
		}
	}
	return 0, false
}

func deathDateFieldScore(fieldNorm string) int {
	switch {
	case strings.Contains(fieldNorm, "date of death"):
		return 100
	case strings.Contains(fieldNorm, "year of death"):
		return 95
	case strings.Contains(fieldNorm, "death date"):
		return 95
	case strings.Contains(fieldNorm, "death") && strings.Contains(fieldNorm, "date"):
		return 90
	case strings.Contains(fieldNorm, "death") && strings.Contains(fieldNorm, "year"):
		return 85
	default:
		return 0
	}
}

var singleYearRe = regexp.MustCompile(`\b(1[0-9]{3}|20[0-9]{2})\b`)

func extractSingleYear(value string) (int, bool) {
	matches := singleYearRe.FindAllString(value, -1)
	if len(matches) == 0 {
		return 0, false
	}
	unique := make(map[string]struct{}, len(matches))
	for _, match := range matches {
		unique[match] = struct{}{}
	}
	if len(unique) != 1 {
		return 0, false
	}
	for yearText := range unique {
		year, err := strconv.Atoi(yearText)
		if err != nil {
			return 0, false
		}
		return year, true
	}
	return 0, false
}

const chatVerifiedResultInstruction = `
You are a helpful assistant answering a community data question for a non-technical user.

Style requirements:
- Answer like a human: natural, warm, and conversational.
- Prefer short paragraphs over bullet points.
- Do NOT use bullet points unless the user explicitly asks for a list.
- Do NOT sound robotic or overly formal.
- Do NOT mention JSON, database, file structure, verification logic, or technical details.

Accuracy requirements:
- Use ONLY the VERIFIED RESULT below.
- Do NOT add facts that are not in the VERIFIED RESULT.
- Keep the exact years and counts unchanged.

Answer format:
- Start with a direct answer in 1-2 sentences.
- If multiple years tie, include all of them naturally in the first sentence.
- Return ONLY the final answer text.
`

func (cs *ChatService) renderVerifiedDeathYearAggregate(ctx context.Context, question string, aggregate *verifiedDeathYearAggregate) (string, error) {
	if aggregate == nil {
		return "", fmt.Errorf("verified aggregate is required")
	}

	var scopeLine string
	if strings.TrimSpace(aggregate.ScopeOriginal) != "" {
		scopeLine = fmt.Sprintf("Scope: %s\n", strings.TrimSpace(aggregate.ScopeOriginal))
	} else {
		scopeLine = "Scope: all records in the current data\n"
	}

	prompt := strings.TrimSpace(chatVerifiedResultInstruction) + "\n\nUser question:\n" + strings.TrimSpace(question) +
		"\n\nVERIFIED RESULT:\n" +
		scopeLine +
		fmt.Sprintf("Highest death count in a single year: %d\n", aggregate.Count) +
		fmt.Sprintf("Year(s): %s\n", joinYearsForPrompt(aggregate.Years))

	answer, _, err := cs.generateFromPrompt(ctx, prompt, nil, "")
	if err != nil {
		return "", err
	}
	return sanitizeAnswerText(answer), nil
}

func joinYearsForPrompt(years []int) string {
	parts := make([]string, 0, len(years))
	for _, year := range years {
		parts = append(parts, strconv.Itoa(year))
	}
	return strings.Join(parts, ", ")
}

func renderVerifiedDeathYearAggregateFallback(aggregate *verifiedDeathYearAggregate) string {
	scopeText := "in the data"
	if strings.TrimSpace(aggregate.ScopeOriginal) != "" {
		scopeText = "at " + strings.TrimSpace(aggregate.ScopeOriginal)
	}
	if len(aggregate.Years) == 1 {
		return fmt.Sprintf("The highest number of deaths %s occurred in %d, when %d students died.", scopeText, aggregate.Years[0], aggregate.Count)
	}

	yearParts := make([]string, 0, len(aggregate.Years))
	for _, year := range aggregate.Years {
		yearParts = append(yearParts, strconv.Itoa(year))
	}
	return fmt.Sprintf("The highest number of deaths %s occurred in %s, with %d students in each of those years.", scopeText, joinNaturalList(yearParts), aggregate.Count)
}

func joinNaturalList(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	case 2:
		return items[0] + " and " + items[1]
	default:
		return strings.Join(items[:len(items)-1], ", ") + ", and " + items[len(items)-1]
	}
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
	models := []string{chatPrimaryModel, chatFallbackModel}
	var lastErr error

	for modelIdx, model := range models {
		attempts := 1
		if modelIdx == 0 {
			attempts = 2
		}

		for attempt := 0; attempt < attempts; attempt++ {
			promptForAttempt := requestPrompt
			if attempt > 0 {
				promptForAttempt = buildRepairChatTurnPrompt(requestPrompt)
			}

			raw, resp, err := cs.generateChatWithModel(ctx, model, prepared, promptForAttempt, audioBytes, audioMime)
			if err != nil {
				lastErr = err
				if modelIdx == 0 && isRateLimit429(err) {
					break
				}
				if attempt == attempts-1 {
					break
				}
				continue
			}

			answer := sanitizeAnswerText(raw)
			if answer != "" && !answerLooksTruncated(answer, firstFinishReason(resp)) {
				return answer, nil
			}

			lastErr = fmt.Errorf("model returned malformed or truncated response")
			if attempt == attempts-1 {
				break
			}
		}
	}

	if lastErr != nil {
		return "", lastErr
	}
	return "", fmt.Errorf("no response from Gemini")
}

func (cs *ChatService) generateChatWithModel(
	ctx context.Context,
	model string,
	prepared *preparedChatDataset,
	requestPrompt string,
	audioBytes []byte,
	audioMime string,
) (string, *genai.GenerateContentResponse, error) {
	cacheEntry, cacheErr := cs.ensureChatContextCache(ctx, model, prepared)
	if cacheErr == nil {
		raw, resp, err := cs.generateChatRequest(ctx, model, requestPrompt, audioBytes, audioMime, newChatGenerateConfig(cacheEntry.Name))
		if err == nil {
			return raw, resp, nil
		}
		if isRateLimit429(err) {
			return "", nil, err
		}
		if isMissingCachedContentError(err) {
			cs.contextCache.Delete(chatContextCacheKey(model, prepared.CacheKey))
		}
	}

	directPrompt := buildDirectChatPrompt(prepared, requestPrompt)
	return cs.generateChatRequest(ctx, model, directPrompt, audioBytes, audioMime, newChatGenerateConfig(""))
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

func buildRepairChatTurnPrompt(basePrompt string) string {
	return strings.TrimSpace(basePrompt) + "\nIMPORTANT: Your previous response was malformed or truncated. Return the full answer again as plain text only. Do not prefix the answer with \"answer:\". Finish the answer in complete sentences."
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

func newChatGenerateConfig(cachedContent string) *genai.GenerateContentConfig {
	return &genai.GenerateContentConfig{
		CachedContent:    cachedContent,
		ResponseMIMEType: "text/plain",
		MaxOutputTokens:  1536,
		Temperature:      float32Ptr(0),
	}
}

func firstFinishReason(resp *genai.GenerateContentResponse) genai.FinishReason {
	if resp == nil || len(resp.Candidates) == 0 || resp.Candidates[0] == nil {
		return genai.FinishReasonUnspecified
	}
	return resp.Candidates[0].FinishReason
}

func answerLooksTruncated(answer string, finishReason genai.FinishReason) bool {
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return true
	}
	if finishReason == genai.FinishReasonMaxTokens {
		return true
	}
	if strings.HasSuffix(answer, "...") || strings.HasSuffix(answer, "…") {
		return true
	}

	lower := strings.ToLower(answer)
	for _, suffix := range []string{
		" a", " an", " the", " and", " or", " of", " with", " in", " at", " to", " from", " by", " for",
		" had a", " had an", " had the", " also had a", " also had an", " also had the",
	} {
		if strings.HasSuffix(lower, suffix) {
			return true
		}
	}

	last := answer[len(answer)-1]
	if last == ',' || last == ':' || last == ';' || last == '(' || last == '-' {
		return true
	}
	return false
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
