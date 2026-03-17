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
			expected: "Hi Athul Narayanan,\n\n" +
				"Your request to add details for John Doe has been approved.\n\n" +
				"Reason / reviewer comment: Looks good\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "rejected without reviewer comment",
			createdUserName: "Athul",
			status:          "rejected",
			firstName:       "Jane",
			lastName:        "Smith",
			reviewerComment: "",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for Jane Smith has been rejected.\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "default updated status for unknown status",
			createdUserName: "Athul",
			status:          "something_else",
			firstName:       "Sam",
			lastName:        "Wilson",
			reviewerComment: "Please review again",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for Sam Wilson has been updated.\n\n" +
				"Reason / reviewer comment: Please review again\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "empty created user falls back to User",
			createdUserName: "",
			status:          "approved",
			firstName:       "John",
			lastName:        "Doe",
			reviewerComment: "",
			expected: "Hi User,\n\n" +
				"Your request to add details for John Doe has been approved.\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "empty first and last name falls back to requested user",
			createdUserName: "Athul",
			status:          "approved",
			firstName:       "",
			lastName:        "",
			reviewerComment: "",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for the requested user has been approved.\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "trims spaces from all inputs",
			createdUserName: "  Athul  ",
			status:          "  approved  ",
			firstName:       "  John  ",
			lastName:        "  Doe  ",
			reviewerComment: "  Looks good  ",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for John Doe has been approved.\n\n" +
				"Reason / reviewer comment: Looks good\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "status comparison is case insensitive",
			createdUserName: "Athul",
			status:          "APPROVED",
			firstName:       "John",
			lastName:        "Doe",
			reviewerComment: "",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for John Doe has been approved.\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
		},
		{
			name:            "single first name only",
			createdUserName: "Athul",
			status:          "rejected",
			firstName:       "Madonna",
			lastName:        "",
			reviewerComment: "Missing data",
			expected: "Hi Athul,\n\n" +
				"Your request to add details for Madonna has been rejected.\n\n" +
				"Reason / reviewer comment: Missing data\n\n" +
				`Please login and see "**Requests -> Add Info Requests**" for details.`,
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
