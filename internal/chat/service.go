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
	"strconv"
	"strings"

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
}

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

func (cs *ChatService) Chat(question string, audioFile *multipart.FileHeader, filename string, communities []string) (string, error) {
	if cs.DB == nil {
		return "", fmt.Errorf("db not initialized")
	}
	if cs.Client == nil {
		return "", fmt.Errorf("genai client not initialized")
	}

	// Fetch latest file version
	var file f.File
	if err := cs.DB.Where("filename = ?", filename).Order("version DESC").First(&file).Error; err != nil {
		return "", fmt.Errorf("file not found")
	}

	// Normalize/filter incoming communities (remove empty entries)
	var filtered []string
	for _, c := range communities {
		c = strings.TrimSpace(c)
		if c != "" {
			filtered = append(filtered, c)
		}
	}

	// Fetch all file data for this file/version, then apply JSON-level filtering by key "First Nation/Community".
	var rawFileData []f.FileData
	if err := cs.DB.Where("file_id = ? AND version = ?", file.ID, file.Version).Find(&rawFileData).Error; err != nil {
		return "", fmt.Errorf("file data not found: %w", err)
	}

	var fileData []f.FileData
	if len(filtered) == 0 {
		fileData = rawFileData
	} else {
		for _, r := range rawFileData {
			if matchesCommunities([]byte(r.RowData), filtered) {
				fileData = append(fileData, r)
			}
		}
	}

	// Build JSON array of rows for the model
	var allRows []json.RawMessage
	for _, row := range fileData {
		allRows = append(allRows, json.RawMessage(row.RowData))
	}

	fileDataJSON, err := json.Marshal(allRows)
	if err != nil {
		return "", fmt.Errorf("failed to marshal file data: %w", err)
	}

	styleInstruction := `
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
`

	prompt := fmt.Sprintf(
		"%s\n\nUser question:\n%s\n\nDATA (only source of truth):\n%s",
		strings.TrimSpace(styleInstruction),
		strings.TrimSpace(question),
		string(fileDataJSON),
	)

	ctx := context.Background()

	// Optional audio path (kept identical behavior, but now uses the reusable generator)
	var audioBytes []byte
	var audioMime string
	if audioFile != nil {
		fh, err := openMultipartFileHook(audioFile)
		if err != nil {
			return "", fmt.Errorf("failed to open audio file: %w", err)
		}
		defer fh.Close()

		audioBytes, err = readAllHook(fh)
		if err != nil {
			return "", fmt.Errorf("failed to read audio file: %w", err)
		}

		audioMime = audioFile.Header.Get("Content-Type")
	}

	answer, usedModel, err := cs.generateFromPrompt(ctx, prompt, audioBytes, audioMime)
	if err != nil {
		return "", fmt.Errorf("generation error (%s): %w", usedModel, err)
	}

	return answer, nil
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

func matchesCommunities(rowData []byte, communities []string) bool {
	var rowMap map[string]interface{}
	if err := json.Unmarshal(rowData, &rowMap); err != nil {
		return false
	}

	key := "First Nation/Community"
	rawVal, ok := rowMap[key]
	if !ok || rawVal == nil {
		return false
	}

	valStr, ok := rawVal.(string)
	if !ok {
		return false
	}

	valStr = strings.TrimSpace(valStr)
	for _, c := range communities {
		if strings.TrimSpace(c) == valStr {
			return true
		}
	}
	return false
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
