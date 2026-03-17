package formsubmission

import "testing"

func TestBuildFormSubmissionReviewEmailBody(t *testing.T) {
	tests := []struct {
		name               string
		createdUserName    string
		formLabel          string
		status             string
		submittedFirstName string
		submittedLastName  string
		reviewerComment    string
		expected           string
	}{
		{
			name:               "approved with comment",
			createdUserName:    "Athul Narayanan",
			formLabel:          "Driver License",
			status:             ReviewStatusApproved,
			submittedFirstName: "John",
			submittedLastName:  "Doe",
			reviewerComment:    "Looks good",
			expected: "Hi Athul Narayanan,\n\n" +
				"Your request to add Driver License for John Doe has been approved.\n\n" +
				"Reason / reviewer comment: Looks good\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "rejected without comment",
			createdUserName:    "Athul",
			formLabel:          "Passport",
			status:             ReviewStatusRejected,
			submittedFirstName: "Jane",
			submittedLastName:  "Smith",
			reviewerComment:    "",
			expected: "Hi Athul,\n\n" +
				"Your request to add Passport for Jane Smith has been rejected.\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "needs more information",
			createdUserName:    "Athul",
			formLabel:          "Health Card",
			status:             ReviewStatusNeedMoreInformation,
			submittedFirstName: "Sam",
			submittedLastName:  "Wilson",
			reviewerComment:    "Upload clearer image",
			expected: "Hi Athul,\n\n" +
				"Your request to add Health Card for Sam Wilson needs more information.\n\n" +
				"Reason / reviewer comment: Upload clearer image\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "unknown status goes to updated",
			createdUserName:    "Athul",
			formLabel:          "SIN",
			status:             "in_review",
			submittedFirstName: "Bruce",
			submittedLastName:  "Wayne",
			reviewerComment:    "Checked",
			expected: "Hi Athul,\n\n" +
				"Your request to add SIN for Bruce Wayne has been updated.\n\n" +
				"Reason / reviewer comment: Checked\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "empty created user falls back to User",
			createdUserName:    "   ",
			formLabel:          "Passport",
			status:             ReviewStatusApproved,
			submittedFirstName: "Clark",
			submittedLastName:  "Kent",
			reviewerComment:    "",
			expected: "Hi User,\n\n" +
				"Your request to add Passport for Clark Kent has been approved.\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "trims all fields and lowercases status",
			createdUserName:    "  Athul  ",
			formLabel:          "  PR Card  ",
			status:             "  APPROVED  ",
			submittedFirstName: "  Peter  ",
			submittedLastName:  "  Parker  ",
			reviewerComment:    "  Looks fine  ",
			expected: "Hi Athul,\n\n" +
				"Your request to add PR Card for Peter Parker has been approved.\n\n" +
				"Reason / reviewer comment: Looks fine\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
		{
			name:               "empty submitted names are kept empty",
			createdUserName:    "Athul",
			formLabel:          "ID Card",
			status:             ReviewStatusRejected,
			submittedFirstName: " ",
			submittedLastName:  " ",
			reviewerComment:    "",
			expected: "Hi Athul,\n\n" +
				"Your request to add ID Card for  has been rejected.\n\n" +
				"Please login and see Requests -> Form Submission Requests for details.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildFormSubmissionReviewEmailBody(
				tt.createdUserName,
				tt.formLabel,
				tt.status,
				tt.submittedFirstName,
				tt.submittedLastName,
				tt.reviewerComment,
			)

			if got != tt.expected {
				t.Fatalf("unexpected body\nexpected:\n%q\n\ngot:\n%q", tt.expected, got)
			}
		})
	}
}
