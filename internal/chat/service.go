package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"

	f "nordik-drive-api/internal/file"

	"google.golang.org/genai"
	"gorm.io/gorm"
)

type ChatService struct {
	DB     *gorm.DB
	APIKey string
	Client *genai.Client
}

func (cs *ChatService) Chat(question string, audioFile *multipart.FileHeader, filename string) (string, error) {
	// Fetch latest file version
	var file f.File
	if err := cs.DB.Where("filename = ?", filename).Order("version DESC").First(&file).Error; err != nil {
		return "", fmt.Errorf("file not found")
	}

	var fileData []f.FileData
	if err := cs.DB.Where("file_id = ? AND version = ?", file.ID, file.Version).Find(&fileData).Error; err != nil {
		return "", fmt.Errorf("file data not found")
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

	// Compose prompt
	prompt := question + "\n\nFile name: " + filename +
		"\n\nAnswer the question based on file data. Please don't take extra data from internet: " + string(fileDataJSON)

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
