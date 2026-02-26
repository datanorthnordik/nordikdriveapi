package formsubmission

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"nordik-drive-api/internal/util"

	"cloud.google.com/go/storage"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type FormSubmissionService struct {
	DB *gorm.DB
}

var uploadBase64ToGCSHook = util.UploadPhotoToGCS
var newFormSubmissionGCSClientHook = func(ctx context.Context) (*storage.Client, error) {
	return storage.NewClient(ctx)
}

func parseFormUploadGSURL(gsURL string) (bucket string, objectPath string, err error) {
	gsURL = strings.TrimSpace(gsURL)
	if gsURL == "" {
		return "", "", fmt.Errorf("empty gs url")
	}
	if !strings.HasPrefix(gsURL, "gs://") {
		return "", "", fmt.Errorf("invalid gs url (must start with gs://): %s", gsURL)
	}

	rest := strings.TrimPrefix(gsURL, "gs://")
	slash := strings.Index(rest, "/")
	if slash < 0 || slash == len(rest)-1 {
		return "", "", fmt.Errorf("invalid gs url format: %s", gsURL)
	}

	bucket = strings.TrimSpace(rest[:slash])
	objectPath = strings.TrimSpace(rest[slash+1:])
	if bucket == "" || objectPath == "" {
		return "", "", fmt.Errorf("invalid gs url format: %s", gsURL)
	}

	return bucket, objectPath, nil
}

func (s *FormSubmissionService) uploadFormFiles(req *SaveFormSubmissionRequest) ([]FormSubmissionUploadInput, []FormSubmissionUploadInput, error) {
	folder := util.SanitizePart(req.FormKey)
	switch folder {
	case "boarding", "boarding_tab", "boarding_home":
		folder = "boarding_home"
	}

	basePrefix := fmt.Sprintf("requests/%s/%d_%d", folder, req.FileID, req.RowID)
	timestamp := time.Now().UTC().Format("20060102150405")
	bucket := "nordik-drive-photos"

	safeBase := func(name string) string {
		name = strings.TrimSpace(name)
		ext := path.Ext(name)
		base := strings.TrimSpace(strings.TrimSuffix(name, ext))
		base = util.SanitizePart(base)
		if base == "" {
			base = "file"
		}
		return base
	}

	type job struct {
		kind string
		idx  int
		item FormSubmissionUploadInput
	}
	type result struct {
		kind string
		idx  int
		item FormSubmissionUploadInput
		err  error
	}

	docs := make([]FormSubmissionUploadInput, len(req.Documents))
	photos := make([]FormSubmissionUploadInput, len(req.Photos))

	jobs := make([]job, 0, len(req.Documents)+len(req.Photos))
	for i, d := range req.Documents {
		jobs = append(jobs, job{kind: "document", idx: i, item: d})
	}
	for i, p := range req.Photos {
		jobs = append(jobs, job{kind: "photo", idx: i, item: p})
	}

	if len(jobs) == 0 {
		return docs, photos, nil
	}

	sem := make(chan struct{}, 4) // 4 parallel uploads
	outCh := make(chan result, len(jobs))
	var wg sync.WaitGroup

	for _, j := range jobs {
		wg.Add(1)

		go func(j job) {
			defer wg.Done()

			sem <- struct{}{}
			defer func() { <-sem }()

			out := j.item

			// Backward compatible: allow already-uploaded URLs
			if strings.TrimSpace(out.DataBase64) == "" {
				if strings.TrimSpace(out.FileURL) == "" {
					outCh <- result{
						err: fmt.Errorf("%s upload %d is missing both data_base64 and file_url", j.kind, j.idx+1),
					}
					return
				}

				outCh <- result{
					kind: j.kind,
					idx:  j.idx,
					item: out,
				}
				return
			}

			ext := util.ExtFromFilenameOrMime(out.FileName, out.MimeType)

			objectName := fmt.Sprintf(
				"%s/%s_%s_%d_%s%s",
				basePrefix,
				j.kind,
				timestamp,
				j.idx+1,
				safeBase(out.FileName),
				ext,
			)

			url, sizeBytes, err := uploadBase64ToGCSHook(
				out.DataBase64,
				bucket,
				objectName,
			)
			if err != nil {
				outCh <- result{
					err: fmt.Errorf("failed to upload %s %q: %w", j.kind, strings.TrimSpace(out.FileName), err),
				}
				return
			}

			out.FileURL = url
			out.FileSizeBytes = sizeBytes

			outCh <- result{
				kind: j.kind,
				idx:  j.idx,
				item: out,
			}
		}(j)
	}

	wg.Wait()
	close(outCh)

	for r := range outCh {
		if r.err != nil {
			return nil, nil, r.err
		}

		if r.kind == "document" {
			docs[r.idx] = r.item
		} else {
			photos[r.idx] = r.item
		}
	}

	return docs, photos, nil
}

func (s *FormSubmissionService) Upsert(req *SaveFormSubmissionRequest) (*GetFormSubmissionResponse, error) {
	if req == nil {
		return nil, errors.New("request is required")
	}
	if req.FileID <= 0 {
		return nil, errors.New("file_id is required")
	}
	if req.RowID <= 0 {
		return nil, errors.New("row_id is required")
	}
	if strings.TrimSpace(req.FormKey) == "" {
		return nil, errors.New("form_key is required")
	}
	if strings.TrimSpace(req.FormLabel) == "" {
		return nil, errors.New("form_label is required")
	}

	// Validate detail keys before upload
	detailKeys := make(map[string]struct{}, len(req.Details))
	for _, d := range req.Details {
		key := strings.TrimSpace(d.DetailKey)
		if key == "" {
			return nil, errors.New("detail_key is required in details")
		}
		detailKeys[key] = struct{}{}
	}

	for _, d := range req.Documents {
		key := strings.TrimSpace(d.DetailKey)
		if key == "" {
			return nil, errors.New("detail_key is required in documents")
		}
		if _, ok := detailKeys[key]; !ok {
			return nil, fmt.Errorf("document detail_key not found in details: %s", key)
		}
	}

	for _, p := range req.Photos {
		key := strings.TrimSpace(p.DetailKey)
		if key == "" {
			return nil, errors.New("detail_key is required in photos")
		}
		if _, ok := detailKeys[key]; !ok {
			return nil, fmt.Errorf("photo detail_key not found in details: %s", key)
		}
	}

	// Upload first (outside DB transaction)
	uploadedDocs, uploadedPhotos, err := s.uploadFormFiles(req)
	if err != nil {
		return nil, err
	}

	err = s.DB.Transaction(func(tx *gorm.DB) error {
		var sub FormSubmission
		findErr := tx.
			Where("file_id = ? AND row_id = ? AND form_key = ?", req.FileID, req.RowID, req.FormKey).
			First(&sub).Error

		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}

		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			sub = FormSubmission{
				FileID:       req.FileID,
				RowID:        req.RowID,
				FileName:     strings.TrimSpace(req.FileName),
				FormKey:      strings.TrimSpace(req.FormKey),
				FormLabel:    strings.TrimSpace(req.FormLabel),
				ConsentText:  req.ConsentText,
				ConsentGiven: req.Consent,
			}
			if err := tx.Create(&sub).Error; err != nil {
				return err
			}
		} else {
			if err := tx.Model(&sub).Updates(map[string]interface{}{
				"file_name":     strings.TrimSpace(req.FileName),
				"form_label":    strings.TrimSpace(req.FormLabel),
				"consent_text":  req.ConsentText,
				"consent_given": req.Consent,
			}).Error; err != nil {
				return err
			}
		}

		// Load existing details once so we can preserve IDs
		var existingDetails []FormSubmissionDetail
		if err := tx.Where("submission_id = ?", sub.ID).Find(&existingDetails).Error; err != nil {
			return err
		}

		existingByKey := make(map[string]FormSubmissionDetail, len(existingDetails))
		detailIDByKey := make(map[string]int64, len(existingDetails))

		for _, ed := range existingDetails {
			key := strings.TrimSpace(ed.DetailKey)
			if key == "" {
				continue
			}
			existingByKey[key] = ed
			detailIDByKey[key] = ed.ID
		}

		// Upsert details by detail_key (preserve old IDs)
		for _, d := range req.Details {
			key := strings.TrimSpace(d.DetailKey)

			raw, err := json.Marshal(d.Value)
			if err != nil {
				return fmt.Errorf("failed to marshal value for detail_key %s: %w", key, err)
			}
			if len(raw) == 0 {
				raw = []byte("null")
			}

			if existing, ok := existingByKey[key]; ok {
				if err := tx.Model(&FormSubmissionDetail{}).
					Where("id = ?", existing.ID).
					Updates(map[string]interface{}{
						"detail_label":     strings.TrimSpace(d.DetailLabel),
						"field_type":       strings.TrimSpace(d.FieldType),
						"consent_required": d.ConsentRequired,
						"value_json":       datatypes.JSON(raw),
					}).Error; err != nil {
					return err
				}
				detailIDByKey[key] = existing.ID
				continue
			}

			row := FormSubmissionDetail{
				SubmissionID:    sub.ID,
				DetailKey:       key,
				DetailLabel:     strings.TrimSpace(d.DetailLabel),
				FieldType:       strings.TrimSpace(d.FieldType),
				ConsentRequired: d.ConsentRequired,
				ValueJSON:       datatypes.JSON(raw),
			}

			if err := tx.Create(&row).Error; err != nil {
				return err
			}

			detailIDByKey[key] = row.ID
		}

		// Insert ONLY newly uploaded files. Do not delete old uploads.
		createUpload := func(uploadType string, in FormSubmissionUploadInput) error {
			key := strings.TrimSpace(in.DetailKey)
			detailID, ok := detailIDByKey[key]
			if !ok {
				return fmt.Errorf("upload detail_key not found in details: %s", key)
			}

			row := FormSubmissionUpload{
				SubmissionID:  sub.ID,
				DetailID:      detailID,
				UploadType:    uploadType,
				FileName:      strings.TrimSpace(in.FileName),
				MimeType:      strings.TrimSpace(in.MimeType),
				FileSizeBytes: in.FileSizeBytes,
				FileURL:       strings.TrimSpace(in.FileURL),
				FileCategory:  strings.TrimSpace(in.FileCategory),
				FileComment:   strings.TrimSpace(in.FileComment),
			}

			return tx.Create(&row).Error
		}

		for _, d := range uploadedDocs {
			if err := createUpload("document", d); err != nil {
				return err
			}
		}

		for _, p := range uploadedPhotos {
			if err := createUpload("photo", p); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	fileID := req.FileID
	return s.GetByRowAndForm(req.RowID, req.FormKey, &fileID)
}

func (s *FormSubmissionService) GetByRowAndForm(rowID int64, formKey string, fileID *int64) (*GetFormSubmissionResponse, error) {
	key := strings.TrimSpace(formKey)
	if rowID <= 0 {
		return nil, errors.New("row_id is required")
	}
	if key == "" {
		return nil, errors.New("form_key is required")
	}

	q := s.DB.Where("row_id = ? AND form_key = ?", rowID, key)
	if fileID != nil && *fileID > 0 {
		q = q.Where("file_id = ?", *fileID)
	}

	var sub FormSubmission
	err := q.First(&sub).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			resp := &GetFormSubmissionResponse{
				Found:     false,
				RowID:     rowID,
				FormKey:   key,
				Details:   []FormSubmissionDetailResponse{},
				Documents: []FormSubmissionUploadResponse{},
				Photos:    []FormSubmissionUploadResponse{},
			}
			if fileID != nil {
				resp.FileID = *fileID
			}
			return resp, nil
		}
		return nil, err
	}

	var details []FormSubmissionDetail
	if err := s.DB.
		Where("submission_id = ?", sub.ID).
		Order("id asc").
		Find(&details).Error; err != nil {
		return nil, err
	}

	var uploads []FormSubmissionUpload
	if err := s.DB.
		Where("submission_id = ?", sub.ID).
		Order("id asc").
		Find(&uploads).Error; err != nil {
		return nil, err
	}

	detailKeyByID := make(map[int64]string, len(details))
	respDetails := make([]FormSubmissionDetailResponse, 0, len(details))

	for _, d := range details {
		detailKeyByID[d.ID] = d.DetailKey

		var val interface{}
		if len(d.ValueJSON) > 0 {
			if err := json.Unmarshal(d.ValueJSON, &val); err != nil {
				return nil, err
			}
		}

		respDetails = append(respDetails, FormSubmissionDetailResponse{
			DetailKey:       d.DetailKey,
			DetailLabel:     d.DetailLabel,
			FieldType:       d.FieldType,
			ConsentRequired: d.ConsentRequired,
			Value:           val,
		})
	}

	respDocs := make([]FormSubmissionUploadResponse, 0)
	respPhotos := make([]FormSubmissionUploadResponse, 0)

	for _, u := range uploads {
		item := FormSubmissionUploadResponse{
			ID:            u.ID,
			DetailKey:     detailKeyByID[u.DetailID],
			FileName:      u.FileName,
			MimeType:      u.MimeType,
			FileSizeBytes: u.FileSizeBytes,
			FileURL:       u.FileURL,
			FileCategory:  u.FileCategory,
			FileComment:   u.FileComment,
		}

		if strings.EqualFold(u.UploadType, "document") {
			respDocs = append(respDocs, item)
		} else {
			respPhotos = append(respPhotos, item)
		}
	}

	return &GetFormSubmissionResponse{
		Found:       true,
		FileID:      sub.FileID,
		RowID:       sub.RowID,
		FileName:    sub.FileName,
		FormKey:     sub.FormKey,
		FormLabel:   sub.FormLabel,
		ConsentText: sub.ConsentText,
		Consent:     sub.ConsentGiven,
		Details:     respDetails,
		Documents:   respDocs,
		Photos:      respPhotos,
	}, nil
}

func (s *FormSubmissionService) GetUploadBytes(id uint) ([]byte, string, string, error) {
	var rec FormSubmissionUpload
	if err := s.DB.First(&rec, id).Error; err != nil {
		return nil, "", "", err
	}

	bucket, objectPath, err := parseFormUploadGSURL(rec.FileURL)
	if err != nil {
		return nil, "", "", err
	}

	ctx := context.Background()
	client, err := newFormSubmissionGCSClientHook(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer client.Close()

	rc, err := client.Bucket(bucket).Object(objectPath).NewReader(ctx)
	if err != nil {
		return nil, "", "", err
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, "", "", err
	}

	contentType := strings.TrimSpace(rc.ContentType())
	if contentType == "" {
		contentType = strings.TrimSpace(rec.MimeType)
	}
	if contentType == "" {
		contentType = http.DetectContentType(data)
	}

	filename := strings.TrimSpace(rec.FileName)
	if filename == "" {
		filename = path.Base(objectPath)
	}
	if filename == "" {
		filename = fmt.Sprintf("upload_%d", rec.ID)
	}

	return data, contentType, filename, nil
}
