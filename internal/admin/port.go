package admin

import (
	"context"
	"io"

	"cloud.google.com/go/storage"
)

type AdminServiceAPI interface {
	SearchFileEditRequests(req AdminFileEditSearchRequest) (*AdminSearchResponse, error)
	GetFileEditRequestDetails(requestID uint) ([]AdminChangeDetailRow, error)
	DownloadUpdates(mode Mode, clauses []Clause, format string) (contentType, filename string, out []byte, err error)
	StreamMediaZip(ctx context.Context, out io.Writer, req AdminDownloadMediaRequest) error
}

type gcsClient interface {
	Bucket(name string) gcsBucket
	Close() error
}
type gcsBucket interface {
	Object(name string) gcsObject
}
type gcsObject interface {
	NewReader(ctx context.Context) (io.ReadCloser, error)
}

type realGCSClient struct{ c *storage.Client }
type realGCSBucket struct{ b *storage.BucketHandle }
type realGCSObject struct{ o *storage.ObjectHandle }

func (r realGCSClient) Bucket(name string) gcsBucket { return realGCSBucket{b: r.c.Bucket(name)} }
func (r realGCSClient) Close() error                 { return r.c.Close() }
func (b realGCSBucket) Object(name string) gcsObject { return realGCSObject{o: b.b.Object(name)} }
func (o realGCSObject) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return o.o.NewReader(ctx)
}

var newGCSClientHook = func(ctx context.Context) (gcsClient, error) {
	c, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	return realGCSClient{c: c}, nil
}
