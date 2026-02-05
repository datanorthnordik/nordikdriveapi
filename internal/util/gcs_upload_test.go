package util

import (
	"testing"
)

func TestSanitizePart(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"  John  ", "john"},
		{"John Doe", "john_doe"},
		{"JOHN_DOE", "john_doe"},
		{"A-B_C", "a-b_c"},
		{"Hello!@#$%^&*()World", "helloworld"},
		{"  ---___  ", "---___"},
		{"", "unknown"},
		{"   ", "unknown"},
		{"नमस्ते", "unknown"},
		{"John.Doe+test@example.com", "johndoetestexamplecom"},
	}

	for _, tt := range tests {
		got := SanitizePart(tt.in)
		if got != tt.want {
			t.Fatalf("SanitizePart(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestTempPrefix(t *testing.T) {
	got := TempPrefix(42, " John ", "Doe!!")
	want := "requests/42_john_doe"
	if got != want {
		t.Fatalf("TempPrefix = %q, want %q", got, want)
	}
}

func TestRowPrefix(t *testing.T) {
	got := RowPrefix(123)
	want := "requests/123"
	if got != want {
		t.Fatalf("RowPrefix = %q, want %q", got, want)
	}
}

func TestPublicGCSURL(t *testing.T) {
	got := PublicGCSURL("my-bucket", "folder/file.jpg")
	want := "https://storage.googleapis.com/my-bucket/folder/file.jpg"
	if got != want {
		t.Fatalf("PublicGCSURL = %q, want %q", got, want)
	}
}

func TestExtractObjectPathFromGCSURL(t *testing.T) {
	tests := []struct {
		name    string
		bucket  string
		raw     string
		want    string
		wantErr bool
	}{
		{
			name:   "storage.googleapis.com format",
			bucket: "my-bucket",
			raw:    "https://storage.googleapis.com/my-bucket/folder/file.jpg",
			want:   "folder/file.jpg",
		},
		{
			name:   "storage.googleapis.com format with query",
			bucket: "my-bucket",
			raw:    "https://storage.googleapis.com/my-bucket/folder/file.jpg?X-Goog-Signature=abc#frag",
			want:   "folder/file.jpg",
		},
		{
			name:   "bucket subdomain format",
			bucket: "my-bucket",
			raw:    "https://my-bucket.storage.googleapis.com/folder/file.jpg",
			want:   "folder/file.jpg",
		},
		{
			name:   "bucket subdomain with query",
			bucket: "my-bucket",
			raw:    "https://my-bucket.storage.googleapis.com/folder/file.jpg?token=1",
			want:   "folder/file.jpg",
		},
		{
			name:   "storage.googleapis.com but bucket not in path returns best effort",
			bucket: "my-bucket",
			raw:    "https://storage.googleapis.com/other-bucket/folder/file.jpg",
			want:   "other-bucket/folder/file.jpg",
		},
		{
			name:   "unknown host returns path best effort",
			bucket: "my-bucket",
			raw:    "https://example.com/my-bucket/folder/file.jpg?x=1",
			want:   "my-bucket/folder/file.jpg",
		},
		{
			name:    "invalid url",
			bucket:  "my-bucket",
			raw:     "%%%not-a-url",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExtractObjectPathFromGCSURL(tt.bucket, tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; got=%q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestReplaceObjectPathInURL(t *testing.T) {
	tests := []struct {
		name     string
		oldURL   string
		bucket   string
		oldObj   string
		newObj   string
		fallback string
		want     string
	}{
		{
			name:     "invalid url returns fallback",
			oldURL:   "%%%bad",
			bucket:   "my-bucket",
			oldObj:   "old.jpg",
			newObj:   "new.jpg",
			fallback: "fallback",
			want:     "fallback",
		},
		{
			name:     "storage.googleapis.com replaces path keeps query",
			oldURL:   "https://storage.googleapis.com/my-bucket/old.jpg?x=1",
			bucket:   "my-bucket",
			oldObj:   "old.jpg",
			newObj:   "folder/new.jpg",
			fallback: "fallback",
			want:     "https://storage.googleapis.com/my-bucket/folder/new.jpg?x=1",
		},
		{
			name:     "bucket subdomain replaces path keeps query",
			oldURL:   "https://my-bucket.storage.googleapis.com/old.jpg?sig=1",
			bucket:   "my-bucket",
			oldObj:   "old.jpg",
			newObj:   "folder/new.jpg",
			fallback: "fallback",
			want:     "https://my-bucket.storage.googleapis.com/folder/new.jpg?sig=1",
		},
		{
			name:     "unknown host returns fallback",
			oldURL:   "https://example.com/my-bucket/old.jpg",
			bucket:   "my-bucket",
			oldObj:   "old.jpg",
			newObj:   "folder/new.jpg",
			fallback: "fallback",
			want:     "fallback",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReplaceObjectPathInURL(tt.oldURL, tt.bucket, tt.oldObj, tt.newObj, tt.fallback)
			if got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}
