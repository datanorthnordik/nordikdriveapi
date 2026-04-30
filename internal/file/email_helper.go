package file

import (
	"fmt"
	"html"
	"strings"
)

const (
	ReviewStatusApproved = "approved"
	ReviewStatusRejected = "rejected"
)

func BuildFileEditRequestReviewEmailBody(
	createdUserName string,
	status string,
	firstName string,
	lastName string,
	reviewerComment string,
	details ...FileEditRequestDetails,
) string {
	createdUserName = strings.TrimSpace(createdUserName)
	status = strings.ToLower(strings.TrimSpace(status))
	firstName = strings.TrimSpace(firstName)
	lastName = strings.TrimSpace(lastName)
	reviewerComment = strings.TrimSpace(reviewerComment)

	if createdUserName == "" {
		createdUserName = "User"
	}

	fullName := strings.TrimSpace(firstName + " " + lastName)
	if fullName == "" {
		fullName = "the requested user"
	}

	var headline string
	switch status {
	case ReviewStatusApproved:
		headline = fmt.Sprintf(
			"Your request to add details for %s has been approved.",
			fullName,
		)
	case ReviewStatusRejected:
		headline = fmt.Sprintf(
			"Your request to add details for %s has been rejected.",
			fullName,
		)
	default:
		headline = fmt.Sprintf(
			"Your request to add details for %s has been updated.",
			fullName,
		)
	}

	var body strings.Builder
	body.WriteString(`<div style="font-family: Arial, Helvetica, sans-serif; color: #1f2933; font-size: 16px; line-height: 1.5; max-width: 760px;">`)
	body.WriteString(fmt.Sprintf(`<p style="margin: 0 0 12px;">Hi %s,</p>`, escapeEmailHTML(createdUserName)))
	body.WriteString(fmt.Sprintf(`<p style="margin: 0 0 16px;">%s</p>`, escapeEmailHTML(headline)))

	if reviewerComment != "" {
		body.WriteString(`<div style="background: #f8fafc; border: 1px solid #d9e2ec; border-left: 4px solid #486581; padding: 12px 14px; margin: 0 0 18px;">`)
		body.WriteString(`<p style="margin: 0 0 4px; font-weight: 700;">Overall reviewer comment</p>`)
		body.WriteString(fmt.Sprintf(`<p style="margin: 0;">%s</p>`, escapeEmailHTML(reviewerComment)))
		body.WriteString(`</div>`)
	}

	if len(details) > 0 {
		body.WriteString(buildFileEditReviewChangesTable(details))
	}

	body.WriteString(`<p style="margin: 18px 0 0;">Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`)
	body.WriteString(`</div>`)

	return body.String()
}

func buildFileEditReviewChangesTable(details []FileEditRequestDetails) string {
	var table strings.Builder

	table.WriteString(`<p style="margin: 0 0 8px; font-weight: 700;">Review summary</p>`)
	table.WriteString(`<table role="presentation" cellspacing="0" cellpadding="0" style="border-collapse: collapse; width: 100%; margin: 0; border: 1px solid #cbd2d9;">`)
	table.WriteString(`<thead>`)
	table.WriteString(`<tr style="background: #f0f4f8;">`)
	for _, heading := range []string{"Field", "Previous value", "Submitted value", "Decision", "Comment"} {
		table.WriteString(fmt.Sprintf(`<th align="left" style="padding: 10px; border: 1px solid #cbd2d9; font-size: 14px; color: #243b53;">%s</th>`, heading))
	}
	table.WriteString(`</tr>`)
	table.WriteString(`</thead>`)
	table.WriteString(`<tbody>`)

	for _, detail := range details {
		status := strings.ToLower(strings.TrimSpace(detail.Status))
		if status == "" {
			status = "pending"
		}

		table.WriteString(`<tr>`)
		table.WriteString(fileEditReviewTableCell(detail.FieldName))
		table.WriteString(fileEditReviewTableCell(detail.OldValue))
		table.WriteString(fileEditReviewTableCell(detail.NewValue))
		table.WriteString(fmt.Sprintf(`<td style="padding: 10px; border: 1px solid #cbd2d9; vertical-align: top;">%s</td>`, fileEditStatusBadge(status)))
		table.WriteString(fileEditReviewTableCell(detail.ReviewComment))
		table.WriteString(`</tr>`)
	}

	table.WriteString(`</tbody>`)
	table.WriteString(`</table>`)

	return table.String()
}

func fileEditReviewTableCell(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		value = "-"
	}

	return fmt.Sprintf(
		`<td style="padding: 10px; border: 1px solid #cbd2d9; vertical-align: top; word-break: break-word;">%s</td>`,
		escapeEmailHTML(value),
	)
}

func fileEditStatusBadge(status string) string {
	label := "Pending"
	style := "background: #fff8c5; color: #5f3b00; border: 1px solid #f0b429;"

	switch status {
	case ReviewStatusApproved:
		label = "Approved"
		style = "background: #e3f8e8; color: #0f5132; border: 1px solid #31a66a;"
	case ReviewStatusRejected:
		label = "Rejected"
		style = "background: #fdecea; color: #842029; border: 1px solid #d64545;"
	}

	return fmt.Sprintf(
		`<span style="display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 13px; font-weight: 700; %s">%s</span>`,
		style,
		label,
	)
}

func escapeEmailHTML(value string) string {
	return html.EscapeString(strings.TrimSpace(value))
}
