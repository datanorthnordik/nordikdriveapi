package file

import (
	"strings"
	"testing"
)

func TestBuildFileEditRequestReviewEmailBody_WithDetailsTable(t *testing.T) {
	body := BuildFileEditRequestReviewEmailBody(
		"Athul Narayanan",
		"completed",
		"John",
		"Doe",
		"Overall looks good",
		FileEditRequestDetails{
			FieldName:     "First Name",
			OldValue:      "Jon",
			NewValue:      "John",
			Status:        "approved",
			ReviewComment: "Correct spelling",
		},
		FileEditRequestDetails{
			FieldName:     "Birth Date",
			OldValue:      "1901",
			NewValue:      "1902",
			Status:        "rejected",
			ReviewComment: "Document does not support this",
		},
	)

	for _, want := range []string{
		"Hi Athul Narayanan,",
		"Your request to add details for John Doe has been reviewed.",
		"Overall reviewer comment",
		"Overall looks good",
		"Review summary",
		"Previous value",
		"Submitted value",
		"Decision",
		"First Name",
		"Jon",
		"John",
		"Approved",
		"Correct spelling",
		"Birth Date",
		"Rejected",
		"Document does not support this",
		"Requests -&gt; Add Info Requests",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected body to contain %q, got %s", want, body)
		}
	}
}

func TestBuildFileEditRequestReviewEmailBody_EscapesDynamicValues(t *testing.T) {
	body := BuildFileEditRequestReviewEmailBody(
		`A <script>`,
		"completed",
		"John",
		"Doe",
		`Use <b>care</b>`,
		FileEditRequestDetails{
			FieldName:     `Name <x>`,
			OldValue:      `A&B`,
			NewValue:      `<New>`,
			Status:        "approved",
			ReviewComment: `Looks "ok"`,
		},
	)

	for _, want := range []string{
		"A &lt;script&gt;",
		"Use &lt;b&gt;care&lt;/b&gt;",
		"Name &lt;x&gt;",
		"A&amp;B",
		"&lt;New&gt;",
		"Looks &#34;ok&#34;",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected escaped body to contain %q, got %s", want, body)
		}
	}

	for _, unsafe := range []string{
		"<script>",
		"<b>care</b>",
		"<New>",
	} {
		if strings.Contains(body, unsafe) {
			t.Fatalf("expected body not to contain unsafe value %q, got %s", unsafe, body)
		}
	}
}

func TestBuildFileEditRequestReviewEmailBody_FallbacksWithoutDetails(t *testing.T) {
	body := BuildFileEditRequestReviewEmailBody("", "something_else", "", "", "")

	for _, want := range []string{
		"Hi User,",
		"Your request to add details for the requested user has been updated.",
		"Requests -&gt; Add Info Requests",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected body to contain %q, got %s", want, body)
		}
	}

	if strings.Contains(body, "Review summary") {
		t.Fatalf("did not expect review table without details, got %s", body)
	}
}
