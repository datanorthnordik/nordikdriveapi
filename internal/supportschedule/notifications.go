package supportschedule

import (
	"fmt"
	"html"
	"strings"
	"time"
)

type supportCallNotificationAudience string

const (
	supportCallRequesterAudience supportCallNotificationAudience = "requester"
	supportCallStaffAudience     supportCallNotificationAudience = "support_staff"
	supportCallManagerAudience   supportCallNotificationAudience = "manager"
)

type supportCallEmailRecipient struct {
	Email    string
	Name     string
	Audience supportCallNotificationAudience
}

func supportUserName(user *SupportUser) string {
	if user == nil {
		return "Not assigned"
	}
	name := strings.TrimSpace(user.FirstName + " " + user.LastName)
	if name == "" {
		return "Not assigned"
	}
	return name
}

func supportCallStatusLabel(status string) string {
	labels := map[string]string{
		RequestStatusPending:             "Pending support review",
		RequestStatusAwaitingApproval:    "Awaiting the selected support person's approval",
		RequestStatusApproved:            "Confirmed",
		RequestStatusAlternativeProposed: "Alternative time proposed",
		RequestStatusRejected:            "Declined",
		RequestStatusCancelled:           "Cancelled",
		RequestStatusCompleted:           "Completed",
	}
	if label, ok := labels[status]; ok {
		return label
	}
	return strings.ReplaceAll(strings.TrimSpace(status), "_", " ")
}

func supportCallRequestTypeLabel(requestType string) string {
	if requestType == RequestTypeSpecificStaff {
		return "Specific support person"
	}
	return "Daily support person"
}

func supportCallTimeRange(start, end *time.Time) string {
	if start == nil || end == nil {
		return "To be confirmed"
	}
	loc, err := time.LoadLocation(DefaultTimeZone)
	if err != nil {
		loc = time.Local
	}
	from, to := start.In(loc), end.In(loc)
	if from.Format("2006-01-02") == to.Format("2006-01-02") {
		return fmt.Sprintf("%s to %s", from.Format("Monday, January 2, 2006 at 3:04 PM MST"), to.Format("3:04 PM MST"))
	}
	return fmt.Sprintf("%s to %s", from.Format("Monday, January 2, 2006 3:04 PM MST"), to.Format("Monday, January 2, 2006 3:04 PM MST"))
}

func supportCallNextStep(request *SupportCallRequest, audience supportCallNotificationAudience) string {
	switch audience {
	case supportCallStaffAudience:
		switch request.Status {
		case RequestStatusPending, RequestStatusAwaitingApproval:
			return "Open Requests and review the call. Approve it, decline it with a reason, or propose an alternative time."
		case RequestStatusApproved:
			return "The call is confirmed. Prepare for the scheduled time and record the actual duration after it is completed."
		case RequestStatusAlternativeProposed:
			return "Wait for the requester to accept the proposed time, or review the request again if another change is needed."
		case RequestStatusCancelled:
			return "No action is needed; this call is no longer reserved on your schedule."
		case RequestStatusCompleted:
			return "The call has been recorded as completed. No further action is needed."
		default:
			return "Review the request in Requests if any follow-up is required."
		}
	case supportCallManagerAudience:
		return "Review the support-call schedule in Requests if reassignment or follow-up is required."
	default:
		switch request.Status {
		case RequestStatusAwaitingApproval:
			return "Your selected support person has been asked to approve the request. You will receive another email when they decide."
		case RequestStatusPending:
			return "Your request has been routed to the daily support person. You will receive another email when it is confirmed or changed."
		case RequestStatusApproved:
			return "Your support call is confirmed. Please join or attend at the scheduled time."
		case RequestStatusAlternativeProposed:
			return "Review the proposed alternative in Requests and accept it if the new time works for you."
		case RequestStatusRejected:
			return "You can submit a new request for another available support time if you still need help."
		case RequestStatusCancelled:
			return "No action is needed. You can submit a new request whenever you need support."
		case RequestStatusCompleted:
			return "The support call has been recorded as completed."
		default:
			return "Review the request in Requests for the latest status."
		}
	}
}

func supportCallNotificationBody(request *SupportCallRequest, recipient supportCallEmailRecipient, event string) string {
	scheduledStart, scheduledEnd := request.PreferredStartTime, request.PreferredEndTime
	if request.Call != nil {
		scheduledStart, scheduledEnd = &request.Call.ScheduledStartTime, &request.Call.ScheduledEndTime
	}
	requestedStaff := "Daily support assignment"
	if request.RequestedStaff != nil {
		requestedStaff = supportUserName(request.RequestedStaff)
	}
	assignedStaff := supportUserName(request.AssignedStaff)
	description := html.EscapeString(strings.TrimSpace(request.Description))
	description = strings.ReplaceAll(description, "\n", "<br/>")
	if description == "" {
		description = "No additional details were provided."
	}
	decisionNote := ""
	if strings.TrimSpace(request.RejectionReason) != "" {
		decisionNote = fmt.Sprintf(`<tr><td style="%s">Update or reason</td><td style="%s">%s</td></tr>`, supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(strings.TrimSpace(request.RejectionReason)))
	}
	alternative := ""
	if request.AlternativeStartTime != nil && request.AlternativeEndTime != nil {
		alternative = fmt.Sprintf(`<tr><td style="%s">Alternative time</td><td style="%s">%s</td></tr>`, supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(supportCallTimeRange(request.AlternativeStartTime, request.AlternativeEndTime)))
	}

	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6;max-width:720px;">`+
			`<h2 style="margin:0 0 8px;color:#003A7A;">%s</h2>`+
			`<p style="margin-top:0;">Hello %s,</p>`+
			`<p>This email concerns support call request <strong>#%d</strong>. Its current status is <strong>%s</strong>.</p>`+
			`<table style="border-collapse:collapse;width:100%%;">`+
			`<tr><td style="%s">Subject</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Requested by</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Request type</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Scheduled time</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Assigned support person</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Requested support person</td><td style="%s">%s</td></tr>`+
			`%s%s`+
			`</table>`+
			`<div style="margin-top:16px;padding:14px;border:1px solid #d1d5db;border-radius:8px;background:#f8fafc;">`+
			`<strong>Request details</strong><br/>%s`+
			`</div>`+
			`<div style="margin-top:16px;padding:14px;border-left:4px solid #003A7A;background:#eff6ff;">`+
			`<strong>What happens next</strong><br/>%s`+
			`</div>`+
			`</div>`,
		html.EscapeString(strings.TrimSpace(event)),
		html.EscapeString(recipient.Name),
		request.ID,
		html.EscapeString(supportCallStatusLabel(request.Status)),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(strings.TrimSpace(request.Subject)),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(supportUserName(&request.RequestedBy)),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(supportCallRequestTypeLabel(request.RequestType)),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(supportCallTimeRange(scheduledStart, scheduledEnd)),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(assignedStaff),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(requestedStaff),
		alternative,
		decisionNote,
		description,
		html.EscapeString(supportCallNextStep(request, recipient.Audience)),
	)
}

func supportCallNotificationSubject(request *SupportCallRequest, event string) string {
	return fmt.Sprintf("[Nordik Drive] Support call #%d — %s", request.ID, strings.TrimSpace(event))
}

const (
	supportCallEmailCellLabelStyle = "padding:8px 12px;border:1px solid #d1d5db;font-weight:700;width:32%;vertical-align:top;"
	supportCallEmailCellStyle      = "padding:8px 12px;border:1px solid #d1d5db;vertical-align:top;"
)

func supportScheduleAssignmentEmailBody(assignment *SupportAssignment, recipient supportCallEmailRecipient, reason string) string {
	previous := supportUserName(assignment.PreviousAssignee)
	current := supportUserName(assignment.PrimaryAssignee)
	nextStep := "Review the schedule in Requests if follow-up is needed."
	if recipient.Audience == supportCallStaffAudience && assignment.PrimaryAssignee != nil && recipient.Email == strings.TrimSpace(assignment.PrimaryAssignee.Email) {
		nextStep = "You are now the primary support person for this date. Review Requests for calls assigned to you."
	}
	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6;max-width:720px;">`+
			`<h2 style="margin:0 0 8px;color:#003A7A;">Support schedule updated</h2>`+
			`<p>Hello %s,</p>`+
			`<p>The primary support assignment for <strong>%s</strong> has changed.</p>`+
			`<table style="border-collapse:collapse;width:100%%;">`+
			`<tr><td style="%s">Previous assignee</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">New assignee</td><td style="%s">%s</td></tr>`+
			`<tr><td style="%s">Reason</td><td style="%s">%s</td></tr>`+
			`</table>`+
			`<div style="margin-top:16px;padding:14px;border-left:4px solid #003A7A;background:#eff6ff;"><strong>What happens next</strong><br/>%s</div>`+
			`</div>`,
		html.EscapeString(recipient.Name),
		html.EscapeString(assignment.AssignmentDate),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(previous),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(current),
		supportCallEmailCellLabelStyle, supportCallEmailCellStyle, html.EscapeString(strings.TrimSpace(reason)),
		html.EscapeString(nextStep),
	)
}
