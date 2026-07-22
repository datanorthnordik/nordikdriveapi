package supportrequest

import "mime/multipart"

type SupportRequestServiceInterface interface {
	Create(req *CreateSupportRequestRequest, userID int, screenshot *multipart.FileHeader) (*CreateSupportRequestResponse, error)
	ListMine(userID, page, pageSize int) (*SupportRequestListResponse, error)
	ListForAdmin(userID, page, pageSize int) (*SupportRequestListResponse, error)
	Update(id int64, req *UpdateSupportRequestRequest, adminUserID int) (*SupportRequest, error)
}

var _ SupportRequestServiceInterface = (*SupportRequestService)(nil)
