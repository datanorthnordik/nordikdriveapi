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

	body := fmt.Sprintf("Hi %s,\n\n%s\n\n", createdUserName, headline)

	if reviewerComment != "" {
		body += fmt.Sprintf("Reason / reviewer comment: %s\n\n", reviewerComment)
	}

	body += "Please login and see Requests -> Form Submission Requests for details."

	return body
}
