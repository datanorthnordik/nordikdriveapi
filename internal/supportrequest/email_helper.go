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

func BuildSupportRequestReceiptEmailBody(req *SupportRequest) string {
	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6">`+
			`<h2 style="margin-bottom:8px;color:#003A7A;">We received your support request</h2>`+
			`<p style="margin-top:0;">Hello %s,</p>`+
			`<p>Thank you for contacting Nordik Drive. Your %s has been received and is currently <strong>Open</strong>.</p>`+
			`<div style="padding:16px;border:1px solid #d1d5db;border-radius:10px;background:#f8fafc;max-width:720px;">`+
			`<div><strong>Request #:</strong> %d</div>`+
			`<div><strong>Subject:</strong> %s</div>`+
			`<div><strong>Submitted:</strong> %s</div>`+
			`</div>`+
			`<p>We will email you again when the request is assigned or closed.</p>`+
			`</div>`,
		html.EscapeString(strings.TrimSpace(req.RequesterName)),
		html.EscapeString(strings.ToLower(supportRequestTypeLabel(req.RequestType))),
		req.ID,
		html.EscapeString(strings.TrimSpace(req.Subject)),
		req.CreatedAt.Format("January 2, 2006 3:04 PM MST"),
	)
}

func BuildSupportRequestStatusEmailBody(req *SupportRequest) string {
	teamLine := ""
	if strings.TrimSpace(req.AssignedTeam) != "" {
		teamLine = fmt.Sprintf(
			`<div><strong>Assigned team:</strong> %s</div>`,
			html.EscapeString(strings.TrimSpace(req.AssignedTeam)),
		)
	}

	noteLine := ""
	if strings.TrimSpace(req.AdminNote) != "" {
		escapedNote := html.EscapeString(strings.TrimSpace(req.AdminNote))
		escapedNote = strings.ReplaceAll(escapedNote, "\n", "<br/>")
		noteLine = fmt.Sprintf(`<div style="margin-top:12px;"><strong>Update:</strong><br/>%s</div>`, escapedNote)
	}

	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6">`+
			`<h2 style="margin-bottom:8px;color:#003A7A;">Your support request has been updated</h2>`+
			`<p style="margin-top:0;">Hello %s,</p>`+
			`<p>Your support request is now <strong>%s</strong>.</p>`+
			`<div style="padding:16px;border:1px solid #d1d5db;border-radius:10px;background:#f8fafc;max-width:720px;">`+
			`<div><strong>Request #:</strong> %d</div>`+
			`<div><strong>Subject:</strong> %s</div>`+
			`%s%s`+
			`</div>`+
			`</div>`,
		html.EscapeString(strings.TrimSpace(req.RequesterName)),
		html.EscapeString(supportRequestStatusLabel(req.Status)),
		req.ID,
		html.EscapeString(strings.TrimSpace(req.Subject)),
		teamLine,
		noteLine,
	)
}

func BuildSupportRequestForwardEmailBody(req *SupportRequest) string {
	screenshotLine := ""
	if strings.TrimSpace(req.ScreenshotURL) != "" {
		screenshotLine = fmt.Sprintf(
			`<div><strong>Screenshot:</strong> <a href="%s">View attached screenshot</a></div>`,
			html.EscapeString(strings.TrimSpace(req.ScreenshotURL)),
		)
	}

	escapedMessage := html.EscapeString(strings.TrimSpace(req.Message))
	escapedMessage = strings.ReplaceAll(escapedMessage, "\n", "<br/>")
	noteLine := ""
	if strings.TrimSpace(req.AdminNote) != "" {
		escapedNote := html.EscapeString(strings.TrimSpace(req.AdminNote))
		escapedNote = strings.ReplaceAll(escapedNote, "\n", "<br/>")
		noteLine = fmt.Sprintf(`<div style="margin-top:16px;"><strong>Admin note</strong><br/>%s</div>`, escapedNote)
	}

	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6">`+
			`<h2 style="margin-bottom:8px;color:#003A7A;">Support request forwarded to %s</h2>`+
			`<p style="margin-top:0;">Please review the request below and follow up through the support workflow.</p>`+
			`<div style="padding:16px;border:1px solid #d1d5db;border-radius:10px;background:#f8fafc;max-width:720px;">`+
			`<div><strong>Request #:</strong> %d</div>`+
			`<div><strong>Submitted by:</strong> %s (%s)</div>`+
			`<div><strong>Type:</strong> %s</div>`+
			`<div><strong>Subject:</strong> %s</div>`+
			`%s`+
			`%s`+
			`</div>`+
			`<div style="margin-top:18px;padding:16px;border:1px solid #d1d5db;border-radius:10px;background:#fff;max-width:720px;">`+
			`<strong>Message</strong><br/>%s`+
			`</div>`+
			`</div>`,
		html.EscapeString(strings.TrimSpace(req.AssignedTeam)),
		req.ID,
		html.EscapeString(strings.TrimSpace(req.RequesterName)),
		html.EscapeString(strings.TrimSpace(req.RequesterEmail)),
		html.EscapeString(supportRequestTypeLabel(req.RequestType)),
		html.EscapeString(strings.TrimSpace(req.Subject)),
		screenshotLine,
		noteLine,
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

func supportRequestStatusLabel(status string) string {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case RequestStatusInProgress:
		return "In Progress"
	case RequestStatusClosed:
		return "Closed"
	default:
		return "Open"
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
