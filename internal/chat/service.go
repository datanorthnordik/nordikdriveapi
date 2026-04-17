package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode"

	f "nordik-drive-api/internal/file"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	"google.golang.org/genai"
	"gorm.io/gorm"
)

type ChatService struct {
	DB     *gorm.DB
	APIKey string
	Client *genai.Client

	// Optional fallbacks (useful for tests or when ClientConfig is missing)
	ProjectID string
	Location  string

	datasetCache sync.Map
	sessionCache sync.Map
}

type chatDatasetCacheEntry struct {
	rows    []cachedChatRow
	rowByID map[int]*cachedChatRow
	schema  chatDatasetSchema
}

type cachedChatRow struct {
	RowID      int
	RowJSON    string
	Community  string
	Values     map[string]string
	Normalized map[string]string
	Temporal   map[string]temporalValue
	Names      []string
}

type chatStructuredResponse struct {
	Answer        string  `json:"answer"`
	MatchedRowRef *string `json:"matched_row_ref"`
}

type preparedChatDataset struct {
	PromptJSON string
	RowRefToID map[string]int
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

var (
	// --- Hooks for unit tests (do NOT remove; makes tests stable and avoids real ADC/Vertex calls) ---

	// Gemini hook (text + multimodal). Default calls real client.
	genaiGenerateContentHook = func(client *genai.Client, ctx context.Context, model string, contents []*genai.Content) (*genai.GenerateContentResponse, error) {
		return client.Models.GenerateContent(ctx, model, contents, nil)
	}

	// Multipart audio hooks (allows forcing open/read errors in tests)
	openMultipartFileHook = func(fh *multipart.FileHeader) (multipart.File, error) {
		return fh.Open()
	}
	readAllHook = func(r io.Reader) ([]byte, error) {
		return io.ReadAll(r)
	}

	// Vertex TTS HTTP hooks
	defaultHTTPClientHook = func(ctx context.Context) (*http.Client, error) {
		return google.DefaultClient(ctx, "https://www.googleapis.com/auth/cloud-platform")
	}
	httpDoHook = func(c *http.Client, req *http.Request) (*http.Response, error) {
		return c.Do(req)
	}
)

func firstTextFromResp(genResp *genai.GenerateContentResponse) string {
	if genResp == nil {
		return ""
	}
	for _, candidate := range genResp.Candidates {
		if candidate.Content == nil {
			continue
		}
		for _, part := range candidate.Content.Parts {
			if strings.TrimSpace(part.Text) != "" {
				return part.Text
			}
		}
	}
	return ""
}

// generateFromPrompt is the single reusable Gemini/Vertex call path.
// - prompt is always required
// - audioBytes/audioMime are optional (Chat can pass them, Describe can pass nil/"")
func (cs *ChatService) generateFromPrompt(
	ctx context.Context,
	prompt string,
	audioBytes []byte,
	audioMime string,
) (answer string, usedModel string, err error) {
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

	contents := []*genai.Content{
		{Role: "user", Parts: parts},
	}

	genResp, usedModel, err := cs.generateWith429Fallback(ctx, contents)
	if err != nil {
		return "", usedModel, err
	}

	out := strings.TrimSpace(firstTextFromResp(genResp))
	if out == "" {
		return "", usedModel, fmt.Errorf("no response from Gemini")
	}

	return out, usedModel, nil
}

func (cs *ChatService) Chat(question string, audioFile *multipart.FileHeader, filename string, communities []string) (*ChatResult, error) {
	return cs.ChatForUser(0, question, audioFile, filename, communities)
	/*
		if false {
		if cs.DB == nil {
			return nil, fmt.Errorf("db not initialized")
		}
		if cs.Client == nil {
			return nil, fmt.Errorf("genai client not initialized")
		}

		// Fetch latest file version
		var file f.File
		if err := cs.DB.Select("id, version").Where("filename = ?", filename).Order("version DESC").First(&file).Error; err != nil {
			return nil, fmt.Errorf("file not found")
		}

		filtered := normalizeCommunities(communities)
		prepared, err := cs.legacyGetPreparedChatDataset(file.ID, file.Version, filtered)
		if err != nil {
			return nil, err
		}

		// Legacy prompt instructions:
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
			- Start with a direct answer in 1–2 sentences.
			- If multiple answers exist, include ALL of them in the first sentence.
			- Combine them naturally (example: "1882 and 1995").
			- Provide as much detail as possible based on the data.
		// End legacy prompt instructions.

		prompt := fmt.Sprintf(
			"%s\n\nStructured output requirements:\n%s\n\nUser question:\n%s\n\nDATA (only source of truth):\n%s",
			strings.TrimSpace(chatStyleInstruction),
			strings.TrimSpace(chatStructuredOutputInstruction),
			strings.TrimSpace(question),
			prepared.PromptJSON,
		)

		ctx := context.Background()

		// Optional audio path (kept identical behavior, but now uses the reusable generator)
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

		return nil, nil
	*/
}

func (cs *ChatService) DescribeRow(rowID int) (string, error) {
	if cs.DB == nil {
		return "", fmt.Errorf("db not initialized")
	}
	if cs.Client == nil {
		return "", fmt.Errorf("genai client not initialized")
	}

	// f.FileData should match your table (id, file_id, row_data, version, etc.)
	var row f.FileData
	if err := cs.DB.First(&row, rowID).Error; err != nil {
		return "", fmt.Errorf("row not found")
	}

	// Empathetic narrative instruction (no hallucinations)
	describeInstruction := `
You are writing a respectful, empathetic narration about ONE individual using ONLY the provided record.

Style requirements:
- Warm, human, and gentle tone.
- 1–2 short paragraphs (no bullet points).
- Do NOT mention JSON, database, tables, ids, or technical details.

Accuracy requirements:
- Use ONLY what appears in the record.
- Do NOT guess or add details (age, family, story, emotions, location history) if not present.
- If a detail is missing (date/cause/community/school), say it is not listed.

Safety:
- Avoid graphic details. If cause/factor is present, describe it briefly and sensitively.

Output:
- Start by stating the person’s name (if available).
- Summarize key fields that exist (community/school/date/cause/notes), and clearly say when something isn’t listed.
`

	// row.RowData is your jsonb string/bytes. If it's []byte in your struct, cast accordingly.
	rowJSON := strings.TrimSpace(string(row.RowData))

	prompt := fmt.Sprintf(
		"%s\n\nRECORD (only source of truth):\n%s",
		strings.TrimSpace(describeInstruction),
		rowJSON,
	)

	ctx := context.Background()
	out, usedModel, err := cs.generateFromPrompt(ctx, prompt, nil, "")
	if err != nil {
		return "", fmt.Errorf("generation error (%s): %w", usedModel, err)
	}
	return out, nil
}

func normalizeCommunities(communities []string) []string {
	filtered := make([]string, 0, len(communities))
	seen := make(map[string]struct{}, len(communities))
	for _, community := range communities {
		community = strings.TrimSpace(community)
		if community == "" {
			continue
		}
		if _, exists := seen[community]; exists {
			continue
		}
		seen[community] = struct{}{}
		filtered = append(filtered, community)
	}
	sort.Strings(filtered)
	return filtered
}

func chatDatasetCacheKey(fileID uint, version int) string {
	return fmt.Sprintf("%d:%d", fileID, version)
}

/*
func chatCommunitiesCacheKey(communities []string) string {
	if len(communities) == 0 {
		return "__all__"
	}
	return strings.Join(communities, "\x1f")
}

func (cs *ChatService) legacyGetPreparedChatDataset(fileID uint, version int, communities []string) (*preparedChatDataset, error) {
	dataset, err := cs.legacyGetOrLoadChatDataset(fileID, version)
	if err != nil {
		return nil, err
	}

	cacheKey := chatCommunitiesCacheKey(communities)
	if cached, ok := dataset.prepared.Load(cacheKey); ok {
		if prepared, ok := cached.(*preparedChatDataset); ok {
			return prepared, nil
		}
	}

	prepared, err := legacyBuildPreparedChatDataset(dataset.rows, communities)
	if err != nil {
		return nil, err
	}

	actual, _ := dataset.prepared.LoadOrStore(cacheKey, prepared)
	if cached, ok := actual.(*preparedChatDataset); ok {
		return cached, nil
	}

	return prepared, nil
}

func (cs *ChatService) legacyGetOrLoadChatDataset(fileID uint, version int) (*chatDatasetCacheEntry, error) {
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
		rows = append(rows, legacyBuildCachedChatRow(rawRow))
	}

	entry := &chatDatasetCacheEntry{rows: rows}
	actual, _ := cs.datasetCache.LoadOrStore(cacheKey, entry)
	if cached, ok := actual.(*chatDatasetCacheEntry); ok {
		return cached, nil
	}

	return entry, nil
}

func legacyBuildCachedChatRow(rawRow f.FileData) cachedChatRow {
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

func legacyBuildPreparedChatDataset(rows []cachedChatRow, communities []string) (*preparedChatDataset, error) {
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

	promptJSON, rowRefToID, err := legacyBuildPromptJSONArray(rows, selectedIndexes)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal file data: %w", err)
	}

	return &preparedChatDataset{
		PromptJSON: promptJSON,
		RowRefToID: rowRefToID,
	}, nil
}

func legacyBuildPromptJSONArray(rows []cachedChatRow, indexes []int) (string, map[string]int, error) {
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
*/

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

func (cs *ChatService) TTS(text string) (*TTSAudio, error) {
	ctx := context.Background()

	// --- Project + Location resolution (with fallbacks) ---
	project := strings.TrimSpace(cs.ProjectID)
	loc := strings.TrimSpace(cs.Location)

	if cs.Client != nil {
		cc := cs.Client.ClientConfig() // NOTE: returns a struct, not a pointer (don't compare to nil)
		if project == "" {
			project = strings.TrimSpace(cc.Project)
		}
		if loc == "" {
			loc = strings.TrimSpace(cc.Location)
		}
	}
	if project == "" {
		project = strings.TrimSpace(os.Getenv("GOOGLE_CLOUD_PROJECT"))
	}

	if project == "" {
		return nil, fmt.Errorf("missing project id (set ChatService.ProjectID or GOOGLE_CLOUD_PROJECT)")
	}
	if loc == "" {
		loc = "global"
	}

	host := "https://aiplatform.googleapis.com"
	if strings.ToLower(loc) != "global" {
		host = fmt.Sprintf("https://%s-aiplatform.googleapis.com", loc)
	}

	url := fmt.Sprintf(
		"%s/v1beta1/projects/%s/locations/%s/publishers/google/models/%s:generateContent",
		host, project, loc, ttsModel,
	)

	prompt := fmt.Sprintf(
		"Style: %s.\nRead the following TEXT exactly as written. Do not add or remove words.\n\nTEXT:\n%s",
		ttsStyleInstr, text,
	)

	reqBody := map[string]any{
		"contents": []any{
			map[string]any{
				"role": "user",
				"parts": []any{
					map[string]any{"text": prompt},
				},
			},
		},
		"generationConfig": map[string]any{
			"responseModalities": []string{"AUDIO"},
			"speechConfig": map[string]any{
				"voiceConfig": map[string]any{
					"prebuiltVoiceConfig": map[string]any{
						"voiceName": ttsVoiceName,
					},
				},
			},
		},
	}

	payload, _ := json.Marshal(reqBody)

	httpClient, err := defaultHTTPClientHook(ctx)
	if err != nil {
		return nil, fmt.Errorf("adc auth error: %w", err)
	}

	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpDoHook(httpClient, req)
	if err != nil {
		return nil, fmt.Errorf("vertex tts request error: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("vertex tts failed (%d): %s", resp.StatusCode, string(raw))
	}

	type inlineData struct {
		MimeType string `json:"mimeType"`
		Data     string `json:"data"`
	}
	type part struct {
		InlineData *inlineData `json:"inlineData,omitempty"`
		Text       string      `json:"text,omitempty"`
	}
	type content struct {
		Parts []part `json:"parts"`
	}
	type candidate struct {
		Content content `json:"content"`
	}
	type genResp struct {
		Candidates []candidate `json:"candidates"`
	}

	var gr genResp
	if err := json.Unmarshal(raw, &gr); err != nil {
		return nil, fmt.Errorf("failed to parse vertex response: %w", err)
	}
	if len(gr.Candidates) == 0 || len(gr.Candidates[0].Content.Parts) == 0 {
		return nil, fmt.Errorf("no audio returned from vertex")
	}

	for _, p := range gr.Candidates[0].Content.Parts {
		if p.InlineData == nil || p.InlineData.Data == "" {
			continue
		}

		pcm, err := decodeBase64Loose(p.InlineData.Data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode audio data: %w", err)
		}

		rate := parseRateFromMime(p.InlineData.MimeType)
		if rate == 0 {
			rate = ttsSampleRate
		}

		wavBytes := pcmToWav(pcm, rate, ttsChannels, ttsBitsPerSample)
		return &TTSAudio{MimeType: "audio/wav", Data: wavBytes}, nil
	}

	return nil, fmt.Errorf("no inline audio returned from vertex")
}

var rateRe = regexp.MustCompile(`rate\s*=\s*(\d+)`)

func parseRateFromMime(m string) int {
	m = strings.ToLower(m)
	if m == "" {
		return 0
	}
	mm := rateRe.FindStringSubmatch(m)
	if len(mm) == 2 {
		if v, err := strconv.Atoi(mm[1]); err == nil && v > 0 {
			return v
		}
	}
	return 0
}

func decodeBase64Loose(s string) ([]byte, error) {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.RawStdEncoding.DecodeString(s)
}

func pcmToWav(pcm []byte, sampleRate, channels, bitsPerSample int) []byte {
	if sampleRate <= 0 {
		sampleRate = ttsSampleRate
	}
	if channels <= 0 {
		channels = ttsChannels
	}
	if bitsPerSample <= 0 {
		bitsPerSample = ttsBitsPerSample
	}

	byteRate := sampleRate * channels * (bitsPerSample / 8)
	blockAlign := channels * (bitsPerSample / 8)
	dataSize := len(pcm)
	riffSize := 36 + dataSize

	buf := new(bytes.Buffer)
	buf.WriteString("RIFF")
	_ = binary.Write(buf, binary.LittleEndian, uint32(riffSize))
	buf.WriteString("WAVE")

	buf.WriteString("fmt ")
	_ = binary.Write(buf, binary.LittleEndian, uint32(16))
	_ = binary.Write(buf, binary.LittleEndian, uint16(1))
	_ = binary.Write(buf, binary.LittleEndian, uint16(channels))
	_ = binary.Write(buf, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(buf, binary.LittleEndian, uint32(byteRate))
	_ = binary.Write(buf, binary.LittleEndian, uint16(blockAlign))
	_ = binary.Write(buf, binary.LittleEndian, uint16(bitsPerSample))

	buf.WriteString("data")
	_ = binary.Write(buf, binary.LittleEndian, uint32(dataSize))
	buf.Write(pcm)

	return buf.Bytes()
}

func isRateLimit429(err error) bool {
	if err == nil {
		return false
	}

	// Standard Google API error type
	var gerr *googleapi.Error
	if errors.As(err, &gerr) && gerr.Code == http.StatusTooManyRequests {
		return true
	}

	// Fallback checks (covers wrapped errors / different error shapes)
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests") ||
		strings.Contains(msg, "resource_exhausted") ||
		strings.Contains(msg, "rate limit") {
		return true
	}

	return false
}

func (cs *ChatService) generateWith429Fallback(
	ctx context.Context,
	contents []*genai.Content,
) (*genai.GenerateContentResponse, string, error) {

	primary := "gemini-2.5-flash"
	fallback := "gemini-2.5-pro"

	// 1) try flash
	resp, err := genaiGenerateContentHook(cs.Client, ctx, primary, contents)
	if err == nil {
		return resp, primary, nil
	}

	// 2) only fallback on 429
	if !isRateLimit429(err) {
		return nil, primary, err
	}

	// 3) try pro
	resp2, err2 := genaiGenerateContentHook(cs.Client, ctx, fallback, contents)
	if err2 == nil {
		return resp2, fallback, nil
	}

	// If fallback also fails, return fallback error (more recent/accurate)
	return nil, fallback, err2
}
