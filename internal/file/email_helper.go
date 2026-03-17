package file

import (
	"fmt"
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

	body := fmt.Sprintf("<p>Hi %s,</p><p>%s</p>", createdUserName, headline)

	if reviewerComment != "" {
		body += fmt.Sprintf("<p>Reason / reviewer comment: %s</p>", reviewerComment)
	}

	body += `<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`

	return body
}
