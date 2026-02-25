package formsubmission

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

type FormSubmissionService struct {
	DB *gorm.DB
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

	err := s.DB.Transaction(func(tx *gorm.DB) error {
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

			if err := tx.Where("submission_id = ?", sub.ID).Delete(&FormSubmissionUpload{}).Error; err != nil {
				return err
			}
			if err := tx.Where("submission_id = ?", sub.ID).Delete(&FormSubmissionDetail{}).Error; err != nil {
				return err
			}
		}

		detailIDByKey := make(map[string]int64, len(req.Details))

		for _, d := range req.Details {
			key := strings.TrimSpace(d.DetailKey)
			if key == "" {
				return errors.New("detail_key is required in details")
			}

			raw, err := json.Marshal(d.Value)
			if err != nil {
				return fmt.Errorf("failed to marshal value for detail_key %s: %w", key, err)
			}
			if len(raw) == 0 {
				raw = []byte("null")
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

		createUpload := func(uploadType string, in FormSubmissionUploadInput) error {
			key := strings.TrimSpace(in.DetailKey)
			if key == "" {
				return errors.New("detail_key is required in uploads")
			}

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

		for _, d := range req.Documents {
			if err := createUpload("document", d); err != nil {
				return err
			}
		}

		for _, p := range req.Photos {
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
