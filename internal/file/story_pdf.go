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
	storyPDFPageWidth       = 612.0
	storyPDFPageHeight      = 792.0
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

func achieverStoryTemplatePDFTitle(firstName, lastName string) string {
	name := strings.TrimSpace(strings.Join([]string{
		strings.TrimSpace(firstName),
		strings.TrimSpace(lastName),
	}, " "))
	if name == "" {
		return "Achiever Story"
	}
	return name
}

func buildAchieverStoryTemplatePDFDataURL(title string, template AchieverStoryTemplateInput) (string, error) {
	pdf, err := buildAchieverStoryTemplatePDF(title, template)
	if err != nil {
		return "", err
	}
	return "data:application/pdf;base64," + base64.StdEncoding.EncodeToString(pdf), nil
}

// buildAchieverStoryTemplatePDF renders the common achiever-story format used
// by the supplied reference document: a blue name banner, a two-column
// biography table, followed by the story and its sources. The layout engine is
// deliberately dependency-free and creates continuation pages as required.
func buildAchieverStoryTemplatePDF(title string, template AchieverStoryTemplateInput) ([]byte, error) {
	if strings.TrimSpace(template.AchieversStory) == "" {
		return nil, fmt.Errorf("achiever's story is empty")
	}

	doc := newAchieverStoryTemplatePDF(title)
	rows := []struct {
		label string
		value string
	}{
		{label: "Date of birth", value: template.DateOfBirth},
		{label: "Date of death", value: template.DateOfDeath},
		{label: "Community", value: template.Community},
		{label: "Parents", value: template.Parents},
		{label: "Siblings", value: template.Siblings},
		{label: "Spouse", value: template.Spouse},
		{label: "Education", value: template.Education},
		{label: "Residential school history", value: template.ResidentialSchoolHistory},
		{label: "Note", value: template.Note},
	}
	for _, row := range rows {
		doc.addTableRow(row.label, templateDisplayValue(row.value))
	}

	doc.addStory(strings.TrimSpace(template.AchieversStory), cleanStorySources(template.Sources))
	return buildTemplatePDFDocument(doc.pageContents()), nil
}

const (
	templatePDFMarginX         = 72.0
	templatePDFContentWidth    = 468.0
	templatePDFLabelWidth      = 112.0
	templatePDFHeaderTop       = 86.0
	templatePDFHeaderHeight    = 24.0
	templatePDFTableTop        = 132.0
	templatePDFBottom          = 720.0
	templatePDFTableFontSize   = 10.5
	templatePDFTableLineHeight = 14.5
)

type achieverStoryTemplatePDF struct {
	title  string
	pages  []strings.Builder
	cursor float64
}

func newAchieverStoryTemplatePDF(title string) *achieverStoryTemplatePDF {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "Achiever Story"
	}
	doc := &achieverStoryTemplatePDF{title: title}
	doc.newPage()
	return doc
}

func (doc *achieverStoryTemplatePDF) newPage() {
	doc.pages = append(doc.pages, strings.Builder{})
	doc.cursor = templatePDFTableTop

	doc.write("q 0.133 0.310 0.525 rg %.2f %.2f %.2f %.2f re f Q\n",
		templatePDFMarginX,
		storyPDFPageHeight-templatePDFHeaderTop-templatePDFHeaderHeight,
		templatePDFContentWidth,
		templatePDFHeaderHeight,
	)
	titleX := (storyPDFPageWidth - approximatePDFTextWidth(doc.title, 16)) / 2
	if titleX < templatePDFMarginX+6 {
		titleX = templatePDFMarginX + 6
	}
	doc.writeText("F2", 16, titleX, templatePDFHeaderTop+18, doc.title, "1 1 1")
}

func (doc *achieverStoryTemplatePDF) addTableRow(label, value string) {
	valueLines := wrapTemplatePDFText(value, templatePDFContentWidth-templatePDFLabelWidth-12, templatePDFTableFontSize)
	if len(valueLines) == 0 {
		valueLines = []string{"Not recorded"}
	}

	firstSegment := true
	for len(valueLines) > 0 {
		segmentLabel := label
		if !firstSegment {
			segmentLabel += " (continued)"
		}
		labelLines := wrapTemplatePDFText(segmentLabel, templatePDFLabelWidth-12, templatePDFTableFontSize*1.1)
		availableLines := int((templatePDFBottom - doc.cursor - 8) / templatePDFTableLineHeight)
		if availableLines < len(labelLines) || availableLines < 1 {
			doc.newPage()
			continue
		}

		lineCount := len(valueLines)
		if lineCount > availableLines {
			lineCount = availableLines
		}
		segmentValues := valueLines[:lineCount]
		valueLines = valueLines[lineCount:]

		rowLineCount := len(segmentValues)
		if len(labelLines) > rowLineCount {
			rowLineCount = len(labelLines)
		}
		rowHeight := float64(rowLineCount)*templatePDFTableLineHeight + 8
		doc.drawTableRow(labelLines, segmentValues, rowHeight)
		doc.cursor += rowHeight
		firstSegment = false

		if len(valueLines) > 0 {
			doc.newPage()
		}
	}
}

func (doc *achieverStoryTemplatePDF) drawTableRow(labelLines, valueLines []string, height float64) {
	bottomY := storyPDFPageHeight - doc.cursor - height
	valueX := templatePDFMarginX + templatePDFLabelWidth

	doc.write("q 0.949 0.961 0.957 rg %.2f %.2f %.2f %.2f re f Q\n",
		templatePDFMarginX, bottomY, templatePDFLabelWidth, height)
	doc.write("q 0.820 0.851 0.843 RG 0.75 w %.2f %.2f %.2f %.2f re S %.2f %.2f m %.2f %.2f l S Q\n",
		templatePDFMarginX, bottomY, templatePDFContentWidth, height,
		valueX, bottomY, valueX, bottomY+height)

	for index, line := range labelLines {
		doc.writeText("F2", templatePDFTableFontSize, templatePDFMarginX+6,
			doc.cursor+15+float64(index)*templatePDFTableLineHeight, line, "0.145 0.184 0.204")
	}
	for index, line := range valueLines {
		doc.writeText("F1", templatePDFTableFontSize, valueX+6,
			doc.cursor+15+float64(index)*templatePDFTableLineHeight, line, "0.145 0.184 0.204")
	}
}

func (doc *achieverStoryTemplatePDF) addStory(story string, sources []string) {
	doc.ensureStorySpace(false)
	doc.cursor += 27
	doc.writeText("F2", 11, templatePDFMarginX, doc.cursor, "Achiever's Story:", "0.08 0.08 0.08")
	doc.cursor += 28

	storyLines := wrapTemplatePDFParagraphs(story, templatePDFContentWidth, 11)
	for _, line := range storyLines {
		if doc.cursor+15 > templatePDFBottom {
			doc.newPage()
			doc.ensureStorySpace(true)
		}
		if line.text != "" {
			wordSpacing := 0.0
			if !line.lastInParagraph {
				if spaces := strings.Count(line.text, " "); spaces > 0 {
					wordSpacing = (templatePDFContentWidth - helveticaPDFTextWidth(line.text, 11)) / float64(spaces)
				}
			}
			doc.writeSpacedText("F1", 11, templatePDFMarginX, doc.cursor, line.text, "0.08 0.08 0.08", wordSpacing)
		}
		doc.cursor += 15
	}

	if len(sources) == 0 {
		return
	}
	if doc.cursor+42 > templatePDFBottom {
		doc.newPage()
	}
	doc.cursor += 14
	doc.writeText("F2", 11, templatePDFMarginX, doc.cursor, "Sources:", "0.08 0.08 0.08")
	doc.cursor += 28

	for sourceIndex, source := range sources {
		for _, line := range wrapTemplatePDFText(source, templatePDFContentWidth, 11) {
			if doc.cursor+15 > templatePDFBottom {
				doc.newPage()
				doc.writeText("F2", 11, templatePDFMarginX, doc.cursor, "Sources (continued):", "0.08 0.08 0.08")
				doc.cursor += 28
			}
			doc.writeText("F1", 11, templatePDFMarginX, doc.cursor, line, "0.02 0.30 0.78")
			doc.cursor += 15
		}
		if sourceIndex < len(sources)-1 {
			doc.cursor += 5
		}
	}
}

func (doc *achieverStoryTemplatePDF) ensureStorySpace(continued bool) {
	if doc.cursor+70 > templatePDFBottom {
		doc.newPage()
	}
	if continued {
		doc.writeText("F2", 11, templatePDFMarginX, doc.cursor, "Achiever's Story (continued):", "0.08 0.08 0.08")
		doc.cursor += 28
	}
}

func (doc *achieverStoryTemplatePDF) write(format string, args ...interface{}) {
	page := &doc.pages[len(doc.pages)-1]
	fmt.Fprintf(page, format, args...)
}

func (doc *achieverStoryTemplatePDF) writeText(font string, size, x, baselineFromTop float64, value, color string) {
	doc.write("BT /%s %.2f Tf %s rg %.2f %.2f Td (%s) Tj ET\n",
		font, size, color, x, storyPDFPageHeight-baselineFromTop, escapeStoryPDFText(value))
}

func (doc *achieverStoryTemplatePDF) writeSpacedText(font string, size, x, baselineFromTop float64, value, color string, wordSpacing float64) {
	doc.write("BT /%s %.2f Tf %s rg %.2f %.2f Td %.3f Tw (%s) Tj ET\n",
		font, size, color, x, storyPDFPageHeight-baselineFromTop, wordSpacing, escapeStoryPDFText(value))
}

func (doc *achieverStoryTemplatePDF) pageContents() []string {
	pages := make([]string, len(doc.pages))
	for index := range doc.pages {
		pages[index] = doc.pages[index].String()
	}
	return pages
}

func templateDisplayValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "Not recorded"
	}
	return value
}

func cleanStorySources(sources []string) []string {
	cleaned := make([]string, 0, len(sources))
	for _, source := range sources {
		if source = strings.TrimSpace(source); source != "" {
			cleaned = append(cleaned, source)
		}
	}
	return cleaned
}

func approximatePDFTextWidth(value string, fontSize float64) float64 {
	width := 0.0
	for _, character := range value {
		switch {
		case unicode.IsSpace(character):
			width += 0.28
		case unicode.IsUpper(character):
			width += 0.64
		case unicode.IsPunct(character):
			width += 0.31
		default:
			width += 0.52
		}
	}
	return width * fontSize
}

type templatePDFLine struct {
	text            string
	lastInParagraph bool
}

// PDF's Helvetica font is proportional, so a character count cannot reliably
// keep text inside the page margins. Widths are in thousandths of an em.
func helveticaPDFTextWidth(value string, fontSize float64) float64 {
	width := 0
	for _, character := range value {
		width += helveticaPDFGlyphWidth(character)
	}
	return float64(width) * fontSize / 1000
}

func helveticaPDFGlyphWidth(character rune) int {
	switch character {
	case ' ', '!', ',', '.', ':', ';', '[', '\\', ']', 'I':
		return 278
	case '\'':
		return 191
	case '\u2018', '\u2019':
		return 222
	case '"', '\u201c', '\u201d':
		return 355
	case '(', ')', '-', '`', 'r':
		return 333
	case '*':
		return 389
	case '/', 'i', 'j', 'l':
		return 222
	case 'f', 't':
		return 278
	case '|':
		return 260
	case '^':
		return 469
	case 'a', 'b', 'd', 'e', 'g', 'h', 'n', 'o', 'p', 'q', 'u', '_', '?', '$', '#',
		'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'L':
		return 556
	case 'J', 'c', 'k', 's', 'v', 'x', 'y', 'z':
		return 500
	case 'M', 'm':
		return 833
	case 'C', 'D', 'H', 'N', 'R', 'U', 'w':
		return 722
	case 'G', 'O', 'Q':
		return 778
	case 'A', 'B', 'E', 'K', 'P', 'S', 'V', 'X', 'Y', '&':
		return 667
	case 'F', 'T', 'Z':
		return 611
	case 'W':
		return 944
	case '+', '<', '=', '>', '~':
		return 584
	case '@':
		return 1015
	case '%':
		return 889
	case '{', '}':
		return 334
	}
	// Unlisted WinAnsi glyphs use a conservative width so they stay in bounds.
	return 667
}

func wrapTemplatePDFText(value string, maxWidth, fontSize float64) []string {
	wrapped := wrapTemplatePDFParagraphs(value, maxWidth, fontSize)
	lines := make([]string, len(wrapped))
	for index, line := range wrapped {
		lines[index] = line.text
	}
	return lines
}

func wrapTemplatePDFParagraphs(value string, maxWidth, fontSize float64) []templatePDFLine {
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\r\n", "\n"), "\r", "\n")
	var lines []templatePDFLine
	for _, paragraph := range strings.Split(value, "\n") {
		words := strings.Fields(paragraph)
		if len(words) == 0 {
			lines = append(lines, templatePDFLine{lastInParagraph: true})
			continue
		}
		current := ""
		for _, word := range words {
			if helveticaPDFTextWidth(word, fontSize) > maxWidth {
				if current != "" {
					lines = append(lines, templatePDFLine{text: current})
					current = ""
				}
				chunk := ""
				for _, character := range word {
					candidate := chunk + string(character)
					if chunk != "" && helveticaPDFTextWidth(candidate, fontSize) > maxWidth {
						lines = append(lines, templatePDFLine{text: chunk})
						chunk = ""
					}
					chunk += string(character)
				}
				current = chunk
				continue
			}
			candidate := word
			if current != "" {
				candidate = current + " " + word
			}
			if helveticaPDFTextWidth(candidate, fontSize) > maxWidth {
				lines = append(lines, templatePDFLine{text: current})
				current = word
			} else {
				current = candidate
			}
		}
		lines = append(lines, templatePDFLine{text: current, lastInParagraph: true})
	}
	for len(lines) > 0 && lines[len(lines)-1].text == "" {
		lines = lines[:len(lines)-1]
	}
	return lines
}

func buildTemplatePDFDocument(pageContents []string) []byte {
	const fixedObjectCount = 4 // catalog, pages, Helvetica, Helvetica-Bold
	objects := make([][]byte, fixedObjectCount+len(pageContents)*2)
	objects[0] = []byte("<< /Type /Catalog /Pages 2 0 R >>")
	objects[2] = []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >>")
	objects[3] = []byte("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >>")

	pageRefs := make([]string, 0, len(pageContents))
	for pageIndex, content := range pageContents {
		pageObjectID := fixedObjectCount + 1 + pageIndex*2
		contentObjectID := pageObjectID + 1
		pageRefs = append(pageRefs, fmt.Sprintf("%d 0 R", pageObjectID))
		objects[pageObjectID-1] = []byte(fmt.Sprintf(
			"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R /F2 4 0 R >> >> /Contents %d 0 R >>",
			contentObjectID,
		))
		objects[contentObjectID-1] = []byte(fmt.Sprintf("<< /Length %d >>\nstream\n%s\nendstream", len(content), content))
	}
	objects[1] = []byte(fmt.Sprintf("<< /Type /Pages /Kids [%s] /Count %d >>", strings.Join(pageRefs, " "), len(pageContents)))

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
	return pdf.Bytes()
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
