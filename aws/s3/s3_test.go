package s3

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	dockertest "gopkg.in/ory-am/dockertest.v3"
)

func prepareS3Container(t *testing.T) (func(), string) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Fatalf("couldn't not connect docker host: %s", err.Error())
	}

	resource, err := pool.Run("atlassianlabs/localstack", "latest", []string{})
	if err != nil {
		t.Fatalf("couldn't start S3 container: %s", err.Error())
	}

	addr := fmt.Sprintf("http://localhost:%s", resource.GetPort("4572/tcp"))

	cleanup := func() {
		if err := pool.Purge(resource); err != nil {
			t.Fatalf("couldn't cleanup S3 container: %s", err.Error())
		}
	}

	if err = pool.Retry(func() error {
		resp, err := http.Get(addr)
		if err != nil {
			return err
		}

		if resp.StatusCode != 200 {
			return fmt.Errorf("didn't return status code 200: %s", resp.Status)
		}

		return nil
	}); err != nil {
		t.Fatalf("couldn't prepare S3 container: %s", err.Error())
	}

	return cleanup, addr
}

func TestS3Put(t *testing.T) {
	cleanup, addr := prepareS3Container(t)
	defer cleanup()

	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("dummy", "dummy", ""),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(endpoints.ApNortheast1RegionID),
		Endpoint:         aws.String(addr),
	}))

	svc := s3.New(sess)
	s := &S3{Service: svc}

	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("test"),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String("ap-northeast-1"),
		},
	})
	if err != nil {
		t.Fatalf("got an err: %s", err.Error())
	}

	expected := "Alice in Wonderland"
	err = s.Put("test", "alice", []byte(expected))
	if err != nil {
		t.Fatalf("got an err: %s", err.Error())
	}

	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String("test"),
		Key:    aws.String("alice"),
	})
	if err != nil {
		t.Fatalf("got an err: %s", err.Error())
	}

	b := new(bytes.Buffer)
	io.Copy(b, result.Body)
	actual := string(b.Bytes())

	if expected != actual {
		t.Fatalf("didn't match S3 object body: expected %s, actual %s", expected, actual)
	}
}
