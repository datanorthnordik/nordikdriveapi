package file

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"
	"unicode"
)

const (
	storyPDFMaxRunesPerLine = 82
	storyPDFLinesPerPage    = 40
)

func achieverStoryPDFTitle(firstName, lastName string) string {
	name := strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(firstName),
		strings.TrimSpace(lastName),
	}, " "))
	if name == "" {
		return "Achiever Story"
	}
	return "Achiever Story - " + name
}

func achieverStoryPDFFilename(firstName, lastName string) string {
	parts := make([]string, 0, 2)
	for _, value := range []string{firstName, lastName} {
		fields := strings.Fields(value)
		if len(fields) > 0 {
			parts = append(parts, strings.Join(fields, "_"))
		}
	}
	if len(parts) == 0 {
		return "achiever_story.pdf"
	}
	return storyStorageFilename(strings.Join(parts, "_") + "_story.pdf")
}

func buildAchieverStoryPDFDataURL(title, storyText string) (string, error) {
	pdf, err := buildAchieverStoryPDF(title, storyText)
	if err != nil {
		return "", err
	}
	return "data:application/pdf;base64," + base64.StdEncoding.EncodeToString(pdf), nil
}

// buildAchieverStoryPDF creates a small, dependency-free PDF using a standard
// Helvetica font. The text is wrapped and split across as many pages as needed.
func buildAchieverStoryPDF(title, storyText string) ([]byte, error) {
	lines := wrapStoryPDFText(storyText, storyPDFMaxRunesPerLine)
	if len(lines) == 0 {
		return nil, fmt.Errorf("story text is empty")
	}

	pageCount := (len(lines) + storyPDFLinesPerPage - 1) / storyPDFLinesPerPage
	objects := make([][]byte, 3+pageCount*2)
	objects[0] = []byte("<< /Type /Catalog /Pages 2 0 R >>")

	pageRefs := make([]string, 0, pageCount)
	for page := 0; page < pageCount; page++ {
		pageObjectID := 4 + page*2
		contentObjectID := pageObjectID + 1
		pageRefs = append(pageRefs, fmt.Sprintf("%d 0 R", pageObjectID))

		start := page * storyPDFLinesPerPage
		end := start + storyPDFLinesPerPage
		if end > len(lines) {
			end = len(lines)
		}

		pageTitle := title
		if page > 0 {
			pageTitle += " (continued)"
		}
		content := buildStoryPDFPageContent(pageTitle, lines[start:end])
		objects[pageObjectID-1] = []byte(fmt.Sprintf(
			"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents %d 0 R >>",
			contentObjectID,
		))
		objects[contentObjectID-1] = []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(content), content))
	}

	objects[1] = []byte(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(pageRefs, " "), pageCount))
	objects[2] = []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")

	var pdf bytes.Buffer
	pdf.WriteString("%PDF-1.4\n%\xE2\xE3\xCF\xD3\n")
	offsets := make([]int, len(objects)+1)
	for index, object := range objects {
		offsets[index+1] = pdf.Len()
		fmt.Fprintf(&pdf, "%d 0 obj\n", index+1)
		pdf.Write(object)
		pdf.WriteString("\nendobj\n")
	}

	xrefOffset := pdf.Len()
	fmt.Fprintf(&pdf, "xref\n0 %d\n", len(objects)+1)
	pdf.WriteString("0000000000 65535 f \n")
	for objectID := 1; objectID <= len(objects); objectID++ {
		fmt.Fprintf(&pdf, "%010d 00000 n \n", offsets[objectID])
	}
	fmt.Fprintf(&pdf, "trailer\n<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objects)+1, xrefOffset)
	return pdf.Bytes(), nil
}

func buildStoryPDFPageContent(title string, lines []string) string {
	var content strings.Builder
	content.WriteString("BT\n/F1 16 Tf\n54 738 Td\n(")
	content.WriteString(escapeStoryPDFText(title))
	content.WriteString(") Tj\n/F1 11 Tf\n0 -30 Td\n")
	for index, line := range lines {
		if index > 0 {
			content.WriteString("0 -15 Td\n")
		}
		content.WriteString("(")
		content.WriteString(escapeStoryPDFText(line))
		content.WriteString(") Tj\n")
	}
	content.WriteString("ET")
	return content.String()
}

func wrapStoryPDFText(value string, limit int) []string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	paragraphs := strings.Split(value, "\n")
	lines := make([]string, 0, len(paragraphs))

	for _, paragraph := range paragraphs {
		words := strings.Fields(paragraph)
		if len(words) == 0 {
			lines = append(lines, "")
			continue
		}

		current := ""
		for _, word := range words {
			for len([]rune(word)) > limit {
				if current != "" {
					lines = append(lines, current)
					current = ""
				}
				runes := []rune(word)
				lines = append(lines, string(runes[:limit]))
				word = string(runes[limit:])
			}

			candidate := word
			if current != "" {
				candidate = current + " " + word
			}
			if len([]rune(candidate)) > limit {
				lines = append(lines, current)
				current = word
			} else {
				current = candidate
			}
		}
		if current != "" {
			lines = append(lines, current)
		}
	}

	for len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func escapeStoryPDFText(value string) string {
	var escaped strings.Builder
	for _, character := range value {
		var encoded byte
		switch character {
		case '\\', '(', ')':
			escaped.WriteByte('\\')
			escaped.WriteRune(character)
			continue
		case '\u2018':
			encoded = 0x91
		case '\u2019':
			encoded = 0x92
		case '\u201C':
			encoded = 0x93
		case '\u201D':
			encoded = 0x94
		case '\u2022':
			encoded = 0x95
		case '\u2013':
			encoded = 0x96
		case '\u2014':
			encoded = 0x97
		case '\u2026':
			encoded = 0x85
		case '\u20AC':
			encoded = 0x80
		case '\u0160':
			encoded = 0x8a
		case '\u0161':
			encoded = 0x9a
		case '\u017D':
			encoded = 0x8e
		case '\u017E':
			encoded = 0x9e
		case '\u0152':
			encoded = 0x8c
		case '\u0153':
			encoded = 0x9c
		case '\u0178':
			encoded = 0x9f
		default:
			switch {
			case character >= 0x20 && character <= 0x7e:
				encoded = byte(character)
			case character >= 0xa0 && character <= 0xff:
				encoded = byte(character)
			case unicode.IsSpace(character):
				encoded = ' '
			default:
				encoded = '?'
			}
		}
		fmt.Fprintf(&escaped, "\\%03o", encoded)
	}
	return escaped.String()
}
