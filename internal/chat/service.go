package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"strings"

	f "nordik-drive-api/internal/file"

	"google.golang.org/genai"
	"gorm.io/gorm"
)

type ChatService struct {
	DB     *gorm.DB
	APIKey string
	Client *genai.Client
}

func (cs *ChatService) Chat(question string, audioFile *multipart.FileHeader, filename string, communities []string) (string, error) {
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

	// Fetch all file data for this file/version, then apply JSON-level filtering by key "First Nation/Home".
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
- Do NOT sound robotic or overly formal. Avoid phrases like "Based on the data provided..." or "According to the dataset..."
- Do NOT mention JSON, database, columns, file name, file version, or any technical details.
- If the answer is not present in the provided data, say so clearly and ask 1 short follow-up question if needed.

Accuracy requirements:
- Use ONLY the provided data below. Do not use the internet or outside knowledge.
- If you are uncertain, be transparent rather than guessing.

Answer format:
- Start with a direct answer in 1–2 sentences.
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
		// Read audio
		fh, err := audioFile.Open()
		if err != nil {
			return "", fmt.Errorf("failed to open audio file: %w", err)
		}
		defer fh.Close()

		audioBytes, err := io.ReadAll(fh)
		if err != nil {
			return "", fmt.Errorf("failed to read audio file: %w", err)
		}

		audioMimeType := audioFile.Header.Get("Content-Type")
		if audioMimeType == "application/octet-stream" {
			audioMimeType = "audio/webm"
		}

		// Generate content (multimodal)
		genResp, err := cs.Client.Models.GenerateContent(ctx, "gemini-2.5-flash", []*genai.Content{
			{
				Role: "user",
				Parts: []*genai.Part{
					{Text: prompt},
					{InlineData: &genai.Blob{Data: audioBytes, MIMEType: audioMimeType}},
				},
			},
		}, nil)

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
			}
		}
	} else {
		// Generate text-only content
		genResp, err := cs.Client.Models.GenerateContent(ctx, "gemini-2.5-flash", []*genai.Content{
			{
				Role: "user",
				Parts: []*genai.Part{
					{Text: prompt},
				},
			},
		}, nil)

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
