package supportrequest

import (
	"fmt"
	"html"
	"strings"
)

func BuildSupportRequestNotificationEmailBody(req *SupportRequest) string {
	screenshotSummary := "No screenshot attached"
	if strings.TrimSpace(req.ScreenshotFileName) != "" {
		screenshotSummary = html.EscapeString(strings.TrimSpace(req.ScreenshotFileName))
		if req.ScreenshotSizeBytes > 0 {
			screenshotSummary = fmt.Sprintf("%s (%s)", screenshotSummary, formatAttachmentSize(req.ScreenshotSizeBytes))
		}
	}

	escapedMessage := html.EscapeString(strings.TrimSpace(req.Message))
	escapedMessage = strings.ReplaceAll(escapedMessage, "\n", "<br/>")

	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6">`+
			`<h2 style="margin-bottom:8px;color:#003A7A;">New %s Submitted</h2>`+
			`<p style="margin-top:0;">A new support request was submitted on the Nordik Drive contact page.</p>`+
			`<table style="border-collapse:collapse;width:100%%;max-width:720px;">`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Submitted By</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Email</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Type</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Subject</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Submitted At</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`<tr><td style="padding:8px 12px;border:1px solid #d1d5db;font-weight:700;">Screenshot</td><td style="padding:8px 12px;border:1px solid #d1d5db;">%s</td></tr>`+
			`</table>`+
			`<div style="margin-top:18px;padding:16px;border:1px solid #d1d5db;border-radius:10px;background:#f8fafc;">`+
			`<div style="font-weight:700;margin-bottom:8px;">Message</div>`+
			`<div>%s</div>`+
			`</div>`+
			`</div>`,
		supportRequestTypeLabel(req.RequestType),
		html.EscapeString(strings.TrimSpace(req.RequesterName)),
		html.EscapeString(strings.TrimSpace(req.RequesterEmail)),
		html.EscapeString(supportRequestTypeLabel(req.RequestType)),
		html.EscapeString(strings.TrimSpace(req.Subject)),
		req.CreatedAt.Format("January 2, 2006 3:04 PM MST"),
		screenshotSummary,
		escapedMessage,
	)
}

func supportRequestTypeLabel(requestType string) string {
	switch normalizeSupportRequestType(requestType) {
	case RequestTypeTechnicalIssue:
		return "Technical Issue"
	default:
		return "Question or Query"
	}
}

func formatAttachmentSize(sizeInBytes int64) string {
	if sizeInBytes < 1024*1024 {
		kilobytes := sizeInBytes / 1024
		if kilobytes < 1 {
			kilobytes = 1
		}
		return fmt.Sprintf("%d KB", kilobytes)
	}

	return fmt.Sprintf("%.1f MB", float64(sizeInBytes)/(1024*1024))
}
