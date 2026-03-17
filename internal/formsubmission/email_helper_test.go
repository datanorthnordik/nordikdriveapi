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
			expected: "<p>Hi Athul Narayanan,</p><p>Your request to add Driver License for John Doe has been approved.</p>" +
				"<p>Reason / reviewer comment: Looks good</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "rejected without comment",
			createdUserName:    "Athul",
			formLabel:          "Passport",
			status:             ReviewStatusRejected,
			submittedFirstName: "Jane",
			submittedLastName:  "Smith",
			reviewerComment:    "",
			expected: "<p>Hi Athul,</p><p>Your request to add Passport for Jane Smith has been rejected.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "needs more information",
			createdUserName:    "Athul",
			formLabel:          "Health Card",
			status:             ReviewStatusNeedMoreInformation,
			submittedFirstName: "Sam",
			submittedLastName:  "Wilson",
			reviewerComment:    "Upload clearer image",
			expected: "<p>Hi Athul,</p><p>Your request to add Health Card for Sam Wilson needs more information.</p>" +
				"<p>Reason / reviewer comment: Upload clearer image</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "unknown status goes to updated",
			createdUserName:    "Athul",
			formLabel:          "SIN",
			status:             "in_review",
			submittedFirstName: "Bruce",
			submittedLastName:  "Wayne",
			reviewerComment:    "Checked",
			expected: "<p>Hi Athul,</p><p>Your request to add SIN for Bruce Wayne has been updated.</p>" +
				"<p>Reason / reviewer comment: Checked</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "empty created user falls back to User",
			createdUserName:    "   ",
			formLabel:          "Passport",
			status:             ReviewStatusApproved,
			submittedFirstName: "Clark",
			submittedLastName:  "Kent",
			reviewerComment:    "",
			expected: "<p>Hi User,</p><p>Your request to add Passport for Clark Kent has been approved.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "trims all fields and lowercases status",
			createdUserName:    "  Athul  ",
			formLabel:          "  PR Card  ",
			status:             "  APPROVED  ",
			submittedFirstName: "  Peter  ",
			submittedLastName:  "  Parker  ",
			reviewerComment:    "  Looks fine  ",
			expected: "<p>Hi Athul,</p><p>Your request to add PR Card for Peter Parker has been approved.</p>" +
				"<p>Reason / reviewer comment: Looks fine</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
		},
		{
			name:               "empty submitted names are kept empty",
			createdUserName:    "Athul",
			formLabel:          "ID Card",
			status:             ReviewStatusRejected,
			submittedFirstName: " ",
			submittedLastName:  " ",
			reviewerComment:    "",
			expected: "<p>Hi Athul,</p><p>Your request to add ID Card for  has been rejected.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Form Submission Requests</b>&quot; for details.</p>`,
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
