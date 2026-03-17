package file

import "testing"

func TestBuildFileEditRequestReviewEmailBody(t *testing.T) {
	tests := []struct {
		name            string
		createdUserName string
		status          string
		firstName       string
		lastName        string
		reviewerComment string
		expected        string
	}{
		{
			name:            "approved with reviewer comment",
			createdUserName: "Athul Narayanan",
			status:          "approved",
			firstName:       "John",
			lastName:        "Doe",
			reviewerComment: "Looks good",
			expected: "<p>Hi Athul Narayanan,</p><p>Your request to add details for John Doe has been approved.</p>" +
				"<p>Reason / reviewer comment: Looks good</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "rejected without reviewer comment",
			createdUserName: "Athul",
			status:          "rejected",
			firstName:       "Jane",
			lastName:        "Smith",
			reviewerComment: "",
			expected: "<p>Hi Athul,</p><p>Your request to add details for Jane Smith has been rejected.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "default updated status for unknown status",
			createdUserName: "Athul",
			status:          "something_else",
			firstName:       "Sam",
			lastName:        "Wilson",
			reviewerComment: "Please review again",
			expected: "<p>Hi Athul,</p><p>Your request to add details for Sam Wilson has been updated.</p>" +
				"<p>Reason / reviewer comment: Please review again</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "empty created user falls back to User",
			createdUserName: "",
			status:          "approved",
			firstName:       "John",
			lastName:        "Doe",
			reviewerComment: "",
			expected: "<p>Hi User,</p><p>Your request to add details for John Doe has been approved.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "empty first and last name falls back to requested user",
			createdUserName: "Athul",
			status:          "approved",
			firstName:       "",
			lastName:        "",
			reviewerComment: "",
			expected: "<p>Hi Athul,</p><p>Your request to add details for the requested user has been approved.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "trims spaces from all inputs",
			createdUserName: "  Athul  ",
			status:          "  approved  ",
			firstName:       "  John  ",
			lastName:        "  Doe  ",
			reviewerComment: "  Looks good  ",
			expected: "<p>Hi Athul,</p><p>Your request to add details for John Doe has been approved.</p>" +
				"<p>Reason / reviewer comment: Looks good</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "status comparison is case insensitive",
			createdUserName: "Athul",
			status:          "APPROVED",
			firstName:       "John",
			lastName:        "Doe",
			reviewerComment: "",
			expected: "<p>Hi Athul,</p><p>Your request to add details for John Doe has been approved.</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
		{
			name:            "single first name only",
			createdUserName: "Athul",
			status:          "rejected",
			firstName:       "Madonna",
			lastName:        "",
			reviewerComment: "Missing data",
			expected: "<p>Hi Athul,</p><p>Your request to add details for Madonna has been rejected.</p>" +
				"<p>Reason / reviewer comment: Missing data</p>" +
				`<p>Please login and see &quot;<b>Requests -&gt; Add Info Requests</b>&quot; for details.</p>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := BuildFileEditRequestReviewEmailBody(
				tt.createdUserName,
				tt.status,
				tt.firstName,
				tt.lastName,
				tt.reviewerComment,
			)

			if got != tt.expected {
				t.Fatalf("unexpected body.\nExpected:\n%q\n\nGot:\n%q", tt.expected, got)
			}
		})
	}
}
