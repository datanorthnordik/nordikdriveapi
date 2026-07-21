package supportrequest

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/mail"
	"nordik-drive-api/internal/mailer"
	"nordik-drive-api/internal/util"
	"path/filepath"
	"strings"
	"time"

	"gorm.io/gorm"
)

type SupportRequestMailer interface {
	Send(to []string, subject, body string) error
	SendWithAttachments(to []string, subject, body string, attachments []mailer.Attachment) error
}

type SupportRequestService struct {
	DB                     *gorm.DB
	Mailer                 SupportRequestMailer
	NotificationRecipients []string
}

type preparedScreenshot struct {
	FileName      string
	MimeType      string
	FileSizeBytes int64
	FileURL       string
	Attachment    mailer.Attachment
}

var ErrInvalidSupportRequest = errors.New("invalid support request")

var uploadSupportRequestScreenshotHook = util.UploadPhotoToGCS
var supportRequestGoHook = func(fn func()) {
	go fn()
}

var triggerSupportRequestNotificationHook = func(
	req *SupportRequest,
	attachment *mailer.Attachment,
	recipients []string,
	sender SupportRequestMailer,
) error {
	if sender == nil || len(recipients) == 0 {
		return nil
	}

	subject := fmt.Sprintf("New %s support request: %s", supportRequestTypeLabel(req.RequestType), req.Subject)
	body := BuildSupportRequestNotificationEmailBody(req)

	if attachment != nil {
		return sender.SendWithAttachments(recipients, subject, body, []mailer.Attachment{*attachment})
	}

	return sender.Send(recipients, subject, body)
}

func normalizeSupportRequestType(value string) string {
	return strings.TrimSpace(strings.ToLower(value))
}

func (s *SupportRequestService) Create(
	req *CreateSupportRequestRequest,
	userID int,
	screenshot *multipart.FileHeader,
) (*CreateSupportRequestResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: request is required", ErrInvalidSupportRequest)
	}
	if userID <= 0 {
		return nil, fmt.Errorf("%w: valid user ID is required", ErrInvalidSupportRequest)
	}

	user, err := s.lookupUser(userID)
	if err != nil {
		return nil, err
	}

	requestType := normalizeSupportRequestType(req.RequestType)
	if _, ok := validSupportRequestTypes[requestType]; !ok {
		return nil, fmt.Errorf("%w: request_type must be question or technical_issue", ErrInvalidSupportRequest)
	}

	requesterName := strings.TrimSpace(req.RequesterName)
	if requesterName == "" {
		requesterName = strings.TrimSpace(user.FirstName + " " + user.LastName)
	}
	if requesterName == "" {
		return nil, fmt.Errorf("%w: requester_name is required", ErrInvalidSupportRequest)
	}
	if len(requesterName) > MaxRequesterNameLength {
		return nil, fmt.Errorf("%w: requester_name must be %d characters or fewer", ErrInvalidSupportRequest, MaxRequesterNameLength)
	}

	requesterEmail := strings.TrimSpace(req.RequesterEmail)
	if requesterEmail == "" {
		requesterEmail = strings.TrimSpace(user.Email)
	}
	if requesterEmail == "" {
		return nil, fmt.Errorf("%w: requester_email is required", ErrInvalidSupportRequest)
	}
	if len(requesterEmail) > MaxRequesterEmailLength {
		return nil, fmt.Errorf("%w: requester_email must be %d characters or fewer", ErrInvalidSupportRequest, MaxRequesterEmailLength)
	}
	if _, err := mail.ParseAddress(requesterEmail); err != nil {
		return nil, fmt.Errorf("%w: requester_email must be a valid email address", ErrInvalidSupportRequest)
	}

	subject := strings.TrimSpace(req.Subject)
	if subject == "" {
		return nil, fmt.Errorf("%w: subject is required", ErrInvalidSupportRequest)
	}
	if len(subject) > MaxSubjectLength {
		return nil, fmt.Errorf("%w: subject must be %d characters or fewer", ErrInvalidSupportRequest, MaxSubjectLength)
	}

	message := strings.TrimSpace(req.Message)
	if message == "" {
		return nil, fmt.Errorf("%w: message is required", ErrInvalidSupportRequest)
	}
	if len(message) > MaxMessageLength {
		return nil, fmt.Errorf("%w: message must be %d characters or fewer", ErrInvalidSupportRequest, MaxMessageLength)
	}

	now := time.Now().UTC()
	prepared, err := s.prepareScreenshotUpload(screenshot, userID, now)
	if err != nil {
		return nil, err
	}

	record := SupportRequest{
		CreatedByID:    userID,
		RequesterName:  requesterName,
		RequesterEmail: requesterEmail,
		RequestType:    requestType,
		Subject:        subject,
		Message:        message,
		Status:         RequestStatusOpen,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if prepared != nil {
		record.ScreenshotFileName = prepared.FileName
		record.ScreenshotMimeType = prepared.MimeType
		record.ScreenshotSizeBytes = prepared.FileSizeBytes
		record.ScreenshotURL = prepared.FileURL
	}

	if err := s.DB.Create(&record).Error; err != nil {
		return nil, err
	}

	if s.Mailer != nil && len(s.NotificationRecipients) > 0 {
		recordCopy := record
		var attachmentCopy *mailer.Attachment
		if prepared != nil {
			attachment := prepared.Attachment
			attachmentCopy = &attachment
		}
		s.triggerNotificationAsync(&recordCopy, attachmentCopy)
	}

	return &CreateSupportRequestResponse{
		ID:      record.ID,
		Message: "Support request received successfully.",
	}, nil
}

func (s *SupportRequestService) lookupUser(userID int) (*SupportRequestUserRef, error) {
	var user SupportRequestUserRef
	if err := s.DB.First(&user, userID).Error; err != nil {
		return nil, err
	}

	return &user, nil
}

func (s *SupportRequestService) prepareScreenshotUpload(
	fileHeader *multipart.FileHeader,
	userID int,
	now time.Time,
) (*preparedScreenshot, error) {
	if fileHeader == nil {
		return nil, nil
	}

	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(io.LimitReader(file, MaxScreenshotSizeInBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("%w: screenshot is empty", ErrInvalidSupportRequest)
	}
	if int64(len(data)) > MaxScreenshotSizeInBytes {
		return nil, fmt.Errorf("%w: screenshot must be 5 MB or smaller", ErrInvalidSupportRequest)
	}

	detectedContentType := strings.TrimSpace(http.DetectContentType(data))
	declaredContentType := strings.TrimSpace(fileHeader.Header.Get("Content-Type"))
	contentType := allowedScreenshotContentType(detectedContentType, declaredContentType)
	if contentType == "" {
		return nil, fmt.Errorf("%w: screenshot must be a PNG, JPG, or WEBP image", ErrInvalidSupportRequest)
	}

	displayFilename, objectPath := buildScreenshotFileNames(fileHeader.Filename, contentType, userID, now)
	base64Body := base64.StdEncoding.EncodeToString(data)
	fileURL, sizeBytes, err := uploadSupportRequestScreenshotHook(
		fmt.Sprintf("data:%s;base64,%s", contentType, base64Body),
		"nordik-drive-photos",
		objectPath,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to upload screenshot: %w", err)
	}

	if sizeBytes <= 0 {
		sizeBytes = int64(len(data))
	}

	return &preparedScreenshot{
		FileName:      displayFilename,
		MimeType:      contentType,
		FileSizeBytes: sizeBytes,
		FileURL:       fileURL,
		Attachment: mailer.Attachment{
			Filename:    displayFilename,
			ContentType: contentType,
			Data:        data,
		},
	}, nil
}

func (s *SupportRequestService) triggerNotificationAsync(
	request *SupportRequest,
	attachment *mailer.Attachment,
) {
	supportRequestGoHook(func() {
		defer func() {
			if recover() != nil {
				_ = s.DB.Model(&SupportRequest{}).
					Where("id = ?", request.ID).
					Update("notification_email_sent", false).Error
			}
		}()

		if err := triggerSupportRequestNotificationHook(
			request,
			attachment,
			s.NotificationRecipients,
			s.Mailer,
		); err != nil {
			return
		}

		_ = s.DB.Model(&SupportRequest{}).
			Where("id = ?", request.ID).
			Update("notification_email_sent", true).Error
	})
}

func allowedScreenshotContentType(values ...string) string {
	for _, value := range values {
		normalized := strings.TrimSpace(strings.ToLower(value))
		if _, ok := validScreenshotMimeTypes[normalized]; ok {
			return normalized
		}
	}

	return ""
}

func buildScreenshotFileNames(originalName string, contentType string, userID int, now time.Time) (string, string) {
	extension := validScreenshotMimeTypes[contentType]

	displayName := strings.TrimSpace(filepath.Base(originalName))
	if displayName == "" {
		displayName = "screenshot" + extension
	}
	if filepath.Ext(displayName) == "" {
		displayName += extension
	}

	baseName := strings.TrimSuffix(displayName, filepath.Ext(displayName))
	safeBaseName := util.SanitizePart(baseName)
	if safeBaseName == "" || safeBaseName == "unknown" {
		safeBaseName = "screenshot"
	}

	objectPath := fmt.Sprintf(
		"support_requests/%d/%s_%s%s",
		userID,
		now.Format("20060102150405"),
		safeBaseName,
		extension,
	)

	return displayName, objectPath
}
