package storage

import (
	"context"
	"log"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Client struct {
	client *minio.Client
	bucket string
}

func NewS3Client(endpoint, accessKey, secretKey, bucket string) (*S3Client, error) {
	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false, // Dev mode (HTTP)
	})
	if err != nil {
		return nil, err
	}

	return &S3Client{
		client: minioClient,
		bucket: bucket,
	}, nil
}

// EnsureBucket creates the bucket if it doesn't exist
func (s *S3Client) EnsureBucket() error {
	ctx := context.Background()
	exists, err := s.client.BucketExists(ctx, s.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return s.client.MakeBucket(ctx, s.bucket, minio.MakeBucketOptions{})
	}
	return nil
}

// UploadFile uploads a local file to the bucket
func (s *S3Client) UploadFile(localPath, objectName string) error {
	ctx := context.Background()

	// Upload the file
	info, err := s.client.FPutObject(ctx, s.bucket, objectName, localPath, minio.PutObjectOptions{
		ContentType: "application/gzip",
	})
	if err != nil {
		return err
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, info.Size)
	return nil
}

// ListFiles returns a list of object names in the bucket
func (s *S3Client) ListFiles(prefix string) ([]string, error) {
	ctx := context.Background()
	var files []string

	objectCh := s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		files = append(files, object.Key)
	}
	return files, nil
}

// DownloadFile downloads an object to a local path (or returns a reader - simplified here to just return reader)
func (s *S3Client) GetObject(objectName string) (*minio.Object, error) {
	ctx := context.Background()
	return s.client.GetObject(ctx, s.bucket, objectName, minio.GetObjectOptions{})
}

// DownloadToLocal downloads the file to a temporary local path for processing
func (s *S3Client) DownloadToLocal(objectName, destPath string) error {
	ctx := context.Background()
	return s.client.FGetObject(ctx, s.bucket, objectName, destPath, minio.GetObjectOptions{})
}
