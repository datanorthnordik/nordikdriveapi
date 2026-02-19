package util

import (
	"path"
	"strings"
)

func ClampComment100(s string) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > 100 {
		return string(r[:100])
	}
	return s
}

func ExtFromFilenameOrMime(filename, mime string) string {
	ext := strings.ToLower(path.Ext(filename))
	if ext != "" {
		return ext
	}
	switch strings.ToLower(mime) {
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/webp":
		return ".webp"
	case "image/heic":
		return ".heic"
	default:
		return ".jpg"
	}
}
