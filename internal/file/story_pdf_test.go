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

func TestBuildAchieverStoryTemplatePDF(t *testing.T) {
	template := AchieverStoryTemplateInput{
		DateOfBirth:              "1956-02-15",
		DateOfDeath:              "Not recorded. Would be 70 in 2026 if living.",
		Community:                "Shoal Lake #126",
		Parents:                  "Evelyn Fair and John Redsky Sr.",
		Siblings:                 "June, Agnes, LeeAnn, John, Gerald, Leslie, Vernon, Earl, and Kelvin",
		Spouse:                   "Not recorded",
		Education:                "Grade 3 recorded at Shingwauk.",
		ResidentialSchoolHistory: "Admitted to Shingwauk Indian Residential School in 1966.",
		Note:                     "Recorded in the Achievers database as a traditional healer.",
		AchieversStory:           strings.Repeat("A survivor story presented in the common format. ", 140),
		Sources:                  []string{"https://example.org/source", "  ", "Community archive"},
	}

	pdf, err := buildAchieverStoryTemplatePDF("Thelma Fair", template)
	if err != nil {
		t.Fatalf("build template PDF: %v", err)
	}
	if !bytes.HasPrefix(pdf, []byte("%PDF-1.4")) {
		t.Fatalf("expected PDF header, got %q", pdf[:min(len(pdf), 16)])
	}
	if pages := strings.Count(string(pdf), "/Type /Page") - 1; pages < 2 {
		t.Fatalf("expected long template story to span multiple pages, got %d", pages)
	}
	if !bytes.Contains(pdf, []byte("0.133 0.310 0.525 rg")) {
		t.Fatal("expected the common blue title banner")
	}
	if !bytes.Contains(pdf, []byte("/BaseFont /Helvetica-Bold")) {
		t.Fatal("expected bold labels in the common template")
	}
}

func TestBuildAchieverStoryTemplatePDFRequiresStory(t *testing.T) {
	if _, err := buildAchieverStoryTemplatePDF("Jane Doe", AchieverStoryTemplateInput{}); err == nil {
		t.Fatal("expected an empty template story to be rejected")
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

func TestValidateAchieverStoryTemplateRequest(t *testing.T) {
	input := AchieverStoryRequestInput{
		FileID:    49,
		RowID:     10,
		StoryType: "text",
		Template: &AchieverStoryTemplateInput{
			DateOfBirth:    "Circa 1956",
			AchieversStory: "A structured achiever story.",
		},
	}

	storyType, contentType, err := validateAchieverStoryRequest(input)
	if err != nil || storyType != "text" || contentType != "application/pdf" {
		t.Fatalf("valid template rejected: type=%q contentType=%q err=%v", storyType, contentType, err)
	}

	input.Template.AchieversStory = " "
	if _, _, err = validateAchieverStoryRequest(input); err == nil || !strings.Contains(err.Error(), "template.achievers_story") {
		t.Fatalf("expected required template story error, got %v", err)
	}
}

func TestAchieverStoryPDFFilename(t *testing.T) {
	if got := achieverStoryPDFFilename("Mary Jane", "O'Connor"); got != "Mary_Jane_O'Connor_story.pdf" {
		t.Fatalf("unexpected PDF filename %q", got)
	}
}
