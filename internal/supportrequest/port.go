package supportrequest

import "mime/multipart"

type SupportRequestServiceInterface interface {
	Create(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error)
}

var _ SupportRequestServiceInterface = (*SupportRequestService)(nil)
