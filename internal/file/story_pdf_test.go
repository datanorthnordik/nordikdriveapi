package file

import (
	"bytes"
	"encoding/base64"
	"strings"
	"testing"
)

func TestBuildAchieverStoryPDF(t *testing.T) {
	text := strings.Repeat("A survivor story with enough words to wrap correctly. ", 90)
	dataURL, err := buildAchieverStoryPDFDataURL("Achiever Story - Jane Doe", text)
	if err != nil {
		t.Fatalf("build PDF: %v", err)
	}

	const prefix = "data:application/pdf;base64,"
	if !strings.HasPrefix(dataURL, prefix) {
		t.Fatalf("unexpected PDF data URL prefix")
	}
	pdf, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(dataURL, prefix))
	if err != nil {
		t.Fatalf("decode PDF: %v", err)
	}
	if !bytes.HasPrefix(pdf, []byte("%PDF-1.4")) || !bytes.Contains(pdf, []byte("%%EOF")) {
		t.Fatalf("generated data is not a complete PDF")
	}
	if !bytes.Contains(pdf, []byte("/Count 2")) {
		t.Fatalf("expected long story to span multiple pages")
	}
}

func TestValidateAchieverStoryVideoRequest(t *testing.T) {
	base := AchieverStoryRequestInput{FileID: 49, RowID: 10, StoryType: "video"}

	linked := base
	linked.VideoURL = "https://videos.example.org/watch/123"
	storyType, contentType, err := validateAchieverStoryRequest(linked)
	if err != nil || storyType != "video" || contentType != "" {
		t.Fatalf("valid video link rejected: type=%q contentType=%q err=%v", storyType, contentType, err)
	}

	uploaded := base
	uploaded.Video = &DocumentInput{
		Filename:   "story.webm",
		MimeType:   "video/webm",
		Size:       1024,
		DataBase64: "data:video/webm;base64,ZA==",
	}
	storyType, contentType, err = validateAchieverStoryRequest(uploaded)
	if err != nil || storyType != "video" || contentType != "video/webm" {
		t.Fatalf("valid video upload rejected: type=%q contentType=%q err=%v", storyType, contentType, err)
	}

	both := uploaded
	both.VideoURL = linked.VideoURL
	if _, _, err = validateAchieverStoryRequest(both); err == nil || !strings.Contains(err.Error(), "either") {
		t.Fatalf("expected link/upload exclusivity error, got %v", err)
	}

	tooLarge := uploaded
	videoCopy := *uploaded.Video
	videoCopy.Size = 20*1024*1024 + 1
	tooLarge.Video = &videoCopy
	if _, _, err = validateAchieverStoryRequest(tooLarge); err == nil || !strings.Contains(err.Error(), "20 MB") {
		t.Fatalf("expected video size error, got %v", err)
	}
}

func TestAchieverStoryPDFFilename(t *testing.T) {
	if got := achieverStoryPDFFilename("Mary Jane", "O'Connor"); got != "Mary_Jane_O'Connor_story.pdf" {
		t.Fatalf("unexpected PDF filename %q", got)
	}
}
