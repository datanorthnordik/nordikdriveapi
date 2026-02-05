package file

import (
	"context"
	"io"
	"mime/multipart"
	"nordik-drive-api/internal/logs"
)

type FileServicePort interface {
	SaveFilesMultipart(uploadedFiles []*multipart.FileHeader, filenames FileUploadInput, userID uint) ([]File, error)

	GetUserRole(userID uint) (string, error)
	GetAllFiles(userID uint, role string) ([]FileWithUser, error)
	GetFileData(filename string, version int) ([]FileData, error)

	DeleteFile(fileID string) (File, error)
	ResetFile(fileID string) (File, error)

	CreateAccess(input []FileAccess) error
	DeleteAccess(accessId string) error
	GetFileAccess(fileId string) ([]FileAccessWithUser, error)
	GetFileHistory(fileId string) ([]FileVersionWithUser, error)

	ReplaceFiles(uploadedFile *multipart.FileHeader, fileID uint, userID uint) error
	RevertFile(filename string, version int, userID uint) error

	CreateEditRequest(input EditRequestInput, userID uint) (*FileEditRequest, error)
	GetEditRequests(statusCSV *string, userID *uint) ([]FileEditRequestWithUser, error)
	ApproveEditRequest(requestID uint, updates []FileEditRequestDetails, userId uint) error

	ReviewPhotos(approved []uint, rejected []uint, reviewer string) error

	GetPhotosByRequest(requestID uint) ([]FileEditRequestPhoto, error)
	GetDocsByRequest(requestID uint) ([]FileEditRequestPhoto, error)
	GetPhotosByRow(rowID uint) ([]FileEditRequestPhoto, error)
	GetDocsByRow(rowID uint) ([]FileEditRequestPhoto, error)

	GetPhotoBytes(photoID uint) ([]byte, string, error)
	GetDocBytes(docID uint) ([]byte, string, string, error)

	// return ReadCloser so controller can stream without accessing handle.Reader field
	OpenMediaHandle(ctx context.Context, id uint, kind string) (io.ReadCloser, string, string, string, error)
}

type LogServicePort interface {
	Log(log logs.SystemLog, payload interface{}) error
}
