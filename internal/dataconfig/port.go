package dataconfig

import "time"

type DataConfigServiceAPI interface {
	GetByFileNameIfModified(fileName string, clientLastModified *time.Time) (*GetConfigResult, error)
}
