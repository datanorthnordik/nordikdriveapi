package util

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

func UploadPhotoToGCS(base64Data, bucketName, objectName string) (string, int64, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", 0, err
	}
	defer client.Close()

	// strip "data:image/jpeg;base64," prefix
	if strings.Contains(base64Data, ",") {
		parts := strings.Split(base64Data, ",")
		base64Data = parts[1]
	}

	data, err := base64.StdEncoding.DecodeString(base64Data)
	if err != nil {
		return "", 0, err
	}

	w := client.Bucket(bucketName).Object(objectName).NewWriter(ctx)
	w.ContentType = "image/jpeg"

	sizeBytes, err := w.Write(data)
	if err != nil {
		return "", 0, err
	}

	if err := w.Close(); err != nil {
		return "", 0, err
	}

	// Return gs:// URL
	url := fmt.Sprintf("gs://%s/%s", bucketName, objectName)

	return url, int64(sizeBytes), nil
}

func SanitizePart(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, " ", "_")
	re := regexp.MustCompile(`[^a-z0-9_\-]`)
	s = re.ReplaceAllString(s, "")
	if s == "" {
		return "unknown"
	}
	return s
}

func TempPrefix(requestID uint, firstName, lastName string) string {
	return fmt.Sprintf(
		"requests/%d_%s_%s",
		requestID,
		SanitizePart(firstName),
		SanitizePart(lastName),
	)
}

func RowPrefix(rowID int) string {
	return fmt.Sprintf("requests/%d", rowID)
}

// Builds a simple GCS URL. If your objects are private and you use signed URLs,
// you should regenerate signed URLs instead of using this.
func PublicGCSURL(bucket, objectPath string) string {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucket, objectPath)
}

// Extract object path from common GCS URL formats.
// Supports:
//   - https://storage.googleapis.com/<bucket>/<object>
//   - https://<bucket>.storage.googleapis.com/<object>
//   - signed URLs (we ignore query params)
func ExtractObjectPathFromGCSURL(bucket, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	u.RawQuery = ""
	u.Fragment = ""

	host := u.Host
	p := strings.TrimPrefix(u.Path, "/")

	// storage.googleapis.com/<bucket>/<object>
	if strings.EqualFold(host, "storage.googleapis.com") {
		prefix := bucket + "/"
		if strings.HasPrefix(p, prefix) {
			return strings.TrimPrefix(p, prefix), nil
		}
		// If bucket isn't in path, return as-is (best effort)
		return p, nil
	}

	// <bucket>.storage.googleapis.com/<object>
	if strings.HasSuffix(strings.ToLower(host), ".storage.googleapis.com") {
		// object is the path
		return p, nil
	}

	// Unknown host; best effort: return path
	return p, nil
}

// Move all objects under srcPrefix/ -> dstPrefix/ (copy+delete)
// Returns mapping oldObjectPath -> newObjectPath (both include full object path inside bucket)
func MoveGCSFolder(bucketName, srcPrefix, dstPrefix string) (map[string]string, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	bkt := client.Bucket(bucketName)

	srcPrefix = strings.TrimSuffix(srcPrefix, "/")
	dstPrefix = strings.TrimSuffix(dstPrefix, "/")

	mapping := map[string]string{}

	it := bkt.Objects(ctx, &storage.Query{Prefix: srcPrefix + "/"})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		oldName := obj.Name
		fileName := path.Base(oldName)
		newName := dstPrefix + "/" + fileName

		// copy
		if _, err := bkt.Object(newName).CopierFrom(bkt.Object(oldName)).Run(ctx); err != nil {
			return nil, err
		}
		// delete old
		if err := bkt.Object(oldName).Delete(ctx); err != nil {
			return nil, err
		}

		mapping[oldName] = newName
	}

	return mapping, nil
}

func ReplaceObjectPathInURL(oldURL, bucket, oldObj, newObj, fallbackURL string) string {
	u, err := url.Parse(oldURL)
	if err != nil {
		return fallbackURL
	}
	host := strings.ToLower(u.Host)

	// storage.googleapis.com/<bucket>/<object>
	if host == "storage.googleapis.com" {
		u.Path = "/" + bucket + "/" + newObj
		return u.String()
	}

	// <bucket>.storage.googleapis.com/<object>
	if strings.HasSuffix(host, ".storage.googleapis.com") {
		u.Path = "/" + newObj
		return u.String()
	}

	// unknown format
	return fallbackURL
}
