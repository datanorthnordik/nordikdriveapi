package supportschedule

import (
	"fmt"
	"html"
	"strings"
	"time"
)

type supportCallNotificationAudience string

const (
	supportCallRequesterAudience   supportCallNotificationAudience = "requester"
	supportCallStaffAudience       supportCallNotificationAudience = "support_staff"
	supportCallFormerStaffAudience supportCallNotificationAudience = "former_support_staff"
	supportCallManagerAudience     supportCallNotificationAudience = "manager"
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
	case supportCallFormerStaffAudience:
		return "You are no longer assigned to this call. The replacement support person and the requester have received the updated meeting details."
	case supportCallStaffAudience:
		switch request.Status {
		case RequestStatusPending, RequestStatusAwaitingApproval:
			return "Open Requests and review the call. Approve it, decline it with a reason, or propose an alternative time."
		case RequestStatusApproved:
			if request.Call != nil && request.Call.ZoomJoinURL != "" {
				return "The call is confirmed. At the scheduled time, open Support Calls in NORDIK Drive and select Start Zoom meeting. Afterward, record the actual duration."
			}
			if request.Call != nil && (request.Call.ZoomSyncStatus == ZoomSyncPending || request.Call.ZoomSyncStatus == ZoomSyncFailed) {
				return "The call is confirmed. The Zoom meeting is being prepared; check Support Calls before the scheduled time."
			}
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
		if request.Status == RequestStatusApproved && request.Call != nil && request.Call.ZoomJoinURL != "" {
			return "The call is confirmed. You may use the participant link below if you need to attend; the assigned support person will host it."
		}
		return "Review the support-call schedule in Requests if reassignment or follow-up is required."
	default:
		switch request.Status {
		case RequestStatusAwaitingApproval:
			return "Your selected support person has been asked to approve the request. You will receive another email when they decide."
		case RequestStatusPending:
			return "We are confirming the support person for this call. You will receive another email when it is confirmed or changed."
		case RequestStatusApproved:
			if request.Call != nil && request.Call.ZoomJoinURL != "" {
				return "Your support call is confirmed. Use the Zoom participant link below at the scheduled time. The assigned support person will admit you from the waiting room."
			}
			if request.Call != nil && (request.Call.ZoomSyncStatus == ZoomSyncPending || request.Call.ZoomSyncStatus == ZoomSyncFailed) {
				return "Your support call is confirmed. The Zoom participant link is being prepared and will appear in Support Calls."
			}
			return "Your support call is confirmed. Please attend at the scheduled time."
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

func supportCallZoomDetails(request *SupportCallRequest, recipient supportCallEmailRecipient) string {
	if request == nil || request.Status != RequestStatusApproved || request.Call == nil || recipient.Audience == supportCallFormerStaffAudience {
		return ""
	}
	call := request.Call
	if strings.TrimSpace(call.ZoomJoinURL) == "" {
		return ""
	}
	meetingID := ""
	if strings.TrimSpace(call.ZoomMeetingID) != "" {
		meetingID = fmt.Sprintf(`<br/><span style="color:#475569;">Meeting ID: %s</span>`, html.EscapeString(call.ZoomMeetingID))
	}
	passcode := ""
	if strings.TrimSpace(call.ZoomPasscode) != "" {
		passcode = fmt.Sprintf(`<br/><span style="color:#475569;">Passcode: %s</span>`, html.EscapeString(call.ZoomPasscode))
	}
	isAssignedHost := recipient.Audience == supportCallStaffAudience && request.AssignedStaff != nil && strings.EqualFold(strings.TrimSpace(recipient.Email), strings.TrimSpace(request.AssignedStaff.Email))
	if isAssignedHost {
		joinURL := html.EscapeString(strings.TrimSpace(call.ZoomJoinURL))
		return `<div style="margin-top:16px;padding:14px;border:1px solid #2563eb;border-radius:8px;background:#eff6ff;">` +
			`<strong>Host this Zoom meeting</strong><br/>Open Support Calls in NORDIK Drive and select <strong>Start Zoom meeting</strong>. ` +
			`The participant link is included below for reference and sharing; it does not replace the host start action.<br/>` +
			fmt.Sprintf(`<a href="%s" style="display:inline-block;margin-top:10px;padding:10px 16px;background:#0757b9;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:700;">Open participant link</a>`, joinURL) +
			fmt.Sprintf(`<br/><span style="display:inline-block;margin-top:10px;color:#475569;">Participant link: <a href="%s">%s</a></span>`, joinURL, joinURL) +
			meetingID + passcode + `</div>`
	}
	joinURL := html.EscapeString(strings.TrimSpace(call.ZoomJoinURL))
	return `<div style="margin-top:16px;padding:14px;border:1px solid #2563eb;border-radius:8px;background:#eff6ff;">` +
		`<strong>Join the Zoom meeting</strong><br/>This is a participant link. The assigned support person is the host.<br/>` +
		fmt.Sprintf(`<a href="%s" style="display:inline-block;margin-top:10px;padding:10px 16px;background:#0757b9;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:700;">Join Zoom meeting</a>`, joinURL) +
		fmt.Sprintf(`<br/><span style="display:inline-block;margin-top:10px;color:#475569;">Participant link: <a href="%s">%s</a></span>`, joinURL, joinURL) +
		meetingID + passcode + `</div>`
}

func supportCallNotificationBody(request *SupportCallRequest, recipient supportCallEmailRecipient, event string) string {
	scheduledStart, scheduledEnd := request.PreferredStartTime, request.PreferredEndTime
	if request.Call != nil {
		scheduledStart, scheduledEnd = &request.Call.ScheduledStartTime, &request.Call.ScheduledEndTime
	}
	description := html.EscapeString(strings.TrimSpace(request.Description))
	description = strings.ReplaceAll(description, "\n", "<br/>")
	if description == "" {
		description = "No additional details were provided."
	}

	intro := "Here is the latest update about this NORDIK Drive support call."
	switch recipient.Audience {
	case supportCallRequesterAudience:
		if request.Status == RequestStatusAwaitingApproval {
			intro = "We received your request and asked the selected support person to review the proposed time."
		} else if request.Status == RequestStatusApproved {
			intro = "Your support call is confirmed. The schedule and meeting details are below."
		}
	case supportCallStaffAudience:
		if request.Status == RequestStatusAwaitingApproval {
			intro = "A user requested a support call with you outside your assigned support day. Please review the proposed time."
		} else if request.Status == RequestStatusApproved {
			intro = "You are assigned to this confirmed support call and will host the Zoom meeting."
		}
	case supportCallFormerStaffAudience:
		intro = "This support call has been moved to another support person, so it is no longer assigned to you."
	}

	rows := supportCallEmailRow("Status", supportCallStatusLabel(request.Status)) +
		supportCallEmailRow("Topic", strings.TrimSpace(request.Subject)) +
		supportCallEmailRow("Person requesting support", supportUserName(&request.RequestedBy)) +
		supportCallEmailRow("Scheduled time", supportCallTimeRange(scheduledStart, scheduledEnd)) +
		supportCallEmailRow("Support person", supportUserName(request.AssignedStaff))
	if strings.TrimSpace(request.RejectionReason) != "" {
		rows += supportCallEmailRow("Update", strings.TrimSpace(request.RejectionReason))
	}
	if request.AlternativeStartTime != nil && request.AlternativeEndTime != nil {
		rows += supportCallEmailRow("Proposed alternative", supportCallTimeRange(request.AlternativeStartTime, request.AlternativeEndTime))
	}
	zoomDetails := supportCallZoomDetails(request, recipient)

	return fmt.Sprintf(
		`<div style="font-family:Arial,sans-serif;color:#1f2937;line-height:1.6;max-width:720px;">`+
			`<h2 style="margin:0 0 8px;color:#003A7A;">%s</h2>`+
			`<p style="margin-top:0;">Hello %s,</p>`+
			`<p>%s</p>`+
			`<table style="border-collapse:collapse;width:100%%;">`+
			`%s`+
			`</table>`+
			`%s`+
			`<div style="margin-top:16px;padding:14px;border:1px solid #d1d5db;border-radius:8px;background:#f8fafc;">`+
			`<strong>What support is needed</strong><br/>%s`+
			`</div>`+
			`<div style="margin-top:16px;padding:14px;border-left:4px solid #003A7A;background:#eff6ff;">`+
			`<strong>What happens next</strong><br/>%s`+
			`</div><p style="margin-top:20px;color:#64748b;font-size:13px;">This message was sent by NORDIK Drive because you are involved in this support call.</p>`+
			`</div>`,
		html.EscapeString(strings.TrimSpace(event)),
		html.EscapeString(recipient.Name),
		html.EscapeString(intro),
		rows,
		zoomDetails,
		description,
		html.EscapeString(supportCallNextStep(request, recipient.Audience)),
	)
}

func supportCallNotificationSubject(_ *SupportCallRequest, event string) string {
	return "[NORDIK Drive] " + strings.TrimSpace(event)
}

func supportCallEmailRow(label, value string) string {
	return fmt.Sprintf(
		`<tr><td style="%s">%s</td><td style="%s">%s</td></tr>`,
		supportCallEmailCellLabelStyle,
		html.EscapeString(strings.TrimSpace(label)),
		supportCallEmailCellStyle,
		html.EscapeString(strings.TrimSpace(value)),
	)
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
