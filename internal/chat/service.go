package chat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
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

	// normalize/filter incoming communities (remove empty entries)
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

	var allRows []json.RawMessage
	for _, row := range fileData {
		allRows = append(allRows, json.RawMessage(row.RowData))
	}

	fileDataJSON, err := json.Marshal(allRows)
	if err != nil {
		return "", fmt.Errorf("failed to marshal file data: %w", err)
	}

	ctx := context.Background()

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

	var response string

	if audioFile != nil {
		fh, err := openMultipartFileHook(audioFile)
		if err != nil {
			return "", fmt.Errorf("failed to open audio file: %w", err)
		}
		defer fh.Close()

		audioBytes, err := readAllHook(fh)
		if err != nil {
			return "", fmt.Errorf("failed to read audio file: %w", err)
		}

		audioMimeType := audioFile.Header.Get("Content-Type")
		if audioMimeType == "application/octet-stream" {
			audioMimeType = "audio/webm"
		}

		genResp, err := genaiGenerateContentHook(cs.Client, ctx, "gemini-2.5-flash", []*genai.Content{
			{
				Role: "user",
				Parts: []*genai.Part{
					{Text: prompt},
					{InlineData: &genai.Blob{Data: audioBytes, MIMEType: audioMimeType}},
				},
			},
		})
		if err != nil {
			return "", fmt.Errorf("generation error: %w", err)
		}

		if len(genResp.Candidates) > 0 {
			for _, candidate := range genResp.Candidates {
				if candidate.Content != nil {
					for _, part := range candidate.Content.Parts {
						if part.Text != "" {
							response = part.Text
							break
						}
					}
				}
				if response != "" {
					break
				}
			}
		}
	} else {
		genResp, err := genaiGenerateContentHook(cs.Client, ctx, "gemini-2.5-flash", []*genai.Content{
			{
				Role: "user",
				Parts: []*genai.Part{
					{Text: prompt},
				},
			},
		})
		if err != nil {
			return "", fmt.Errorf("generation error: %w", err)
		}

		if len(genResp.Candidates) > 0 {
			for _, candidate := range genResp.Candidates {
				if candidate.Content != nil {
					for _, part := range candidate.Content.Parts {
						if part.Text != "" {
							response = part.Text
							break
						}
					}
				}
				if response != "" {
					break
				}
			}
		}
	}

	if response == "" {
		return "", fmt.Errorf("no response from Gemini")
	}
	return response, nil
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
