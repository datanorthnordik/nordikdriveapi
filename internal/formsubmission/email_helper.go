package formsubmission

import (
	"fmt"
	"strings"
)

func BuildFormSubmissionReviewEmailBody(
	createdUserName string,
	formLabel string,
	status string,
	submittedFirstName string,
	submittedLastName string,
	reviewerComment string,
) string {
	createdUserName = strings.TrimSpace(createdUserName)
	formLabel = strings.TrimSpace(formLabel)
	status = strings.ToLower(strings.TrimSpace(status))
	submittedFirstName = strings.TrimSpace(submittedFirstName)
	submittedLastName = strings.TrimSpace(submittedLastName)
	reviewerComment = strings.TrimSpace(reviewerComment)

	if createdUserName == "" {
		createdUserName = "User"
	}

	fullName := strings.TrimSpace(submittedFirstName + " " + submittedLastName)

	var headline string
	switch status {
	case ReviewStatusApproved:
		headline = fmt.Sprintf(
			"Your request to add %s for %s has been approved.",
			formLabel,
			fullName,
		)
	case ReviewStatusRejected:
		headline = fmt.Sprintf(
			"Your request to add %s for %s has been rejected.",
			formLabel,
			fullName,
		)
	case ReviewStatusNeedMoreInformation:
		headline = fmt.Sprintf(
			"Your request to add %s for %s needs more information.",
			formLabel,
			fullName,
		)
	default:
		headline = fmt.Sprintf(
			"Your request to add %s for %s has been updated.",
			formLabel,
			fullName,
		)
	}

	body := fmt.Sprintf("<p>Hi %s,</p><p>%s</p>", createdUserName, headline)

	if reviewerComment != "" {
		body += fmt.Sprintf("<p>Reason / reviewer comment: %s</p>", reviewerComment)
	}

	body += `<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`

	return body
}
