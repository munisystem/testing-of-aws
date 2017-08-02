package s3

import (
	"bytes"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	awspkg "github.com/munisystem/testing-of-aws/aws"
)

type S3 struct {
	Service *s3.S3
}

func NewClient() *S3 {
	return &S3{
		Service: s3.New(awspkg.Session()),
	}
}

func (s *S3) Put(bucket, key string, body []byte) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	}

	if _, err := s.Service.PutObject(input); err != nil {
		return fmt.Errorf("Faild to add object to S3 (bucket: %s, key: %s): %s", bucket, key, err.Error())
	}

	return nil
}
