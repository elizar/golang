package utils

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// GetBodyFromS3Key gets the s3 object with the provided
// s3 key.
func GetBodyFromS3Key(key string) ([]byte, error) {
	// Fetch item from s3 usig the extracted key
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	params := &s3.GetObjectInput{
		Bucket: aws.String("stats-lambda-v2"),
		Key:    aws.String(key),
	}

	obj, err := svc.GetObject(params)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(obj.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

// S3Upload uploads the given list of User to S3 with
// a given bucket, key
func S3Upload(item interface{}, bucket string, key string) error {
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	// Before uploading to S3, we need to compress the item
	// and convert to hex digested string.
	data, err := Base64Compress(item)
	if err != nil {
		LogIt(fmt.Sprintf("Cannot compress due to error: %s", err.Error()))
		return err
	}

	params := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),                    // Required
		Key:         aws.String(key),                       // Required
		Expires:     aws.Time(time.Now().AddDate(0, 0, 1)), // 1 day from now
		Body:        bytes.NewReader([]byte(data)),
		ContentType: aws.String("text/plain"),
	}

	_, err = svc.PutObject(params)
	if err != nil {
		LogIt(fmt.Sprintf("Failed to upload item to S3: %s", err.Error()))
		return err
	}

	return nil
}

// Base64Compress compresses the given data and returns a
// base64 string representation of the compressed data.
func Base64Compress(data interface{}) (string, error) {
	d, err := json.Marshal(data)

	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	w.Write(d)
	w.Close()

	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

// DecompressBas64 decompresses base64 encoded and zlib compressed
// data.
func DecompressBas64(data []byte) (io.ReadCloser, error) {
	d, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil, err
	}

	b := bytes.NewReader(d)

	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return r, nil
}

// LogIt ...
func LogIt(message string) {
	logMessage := fmt.Sprintf("%s - %s", time.Now().Format(time.RFC3339), message)
	fmt.Fprintln(os.Stderr, logMessage)
}

// GetOffset gets the timezone offset that a given hour (between 1-24) currently falls under
func GetOffset(st time.Time, hour int) (offset int) {
	offset = 0

	// Server Offset
	_, so := st.Zone()
	// UTC Hour
	UTCHour := st.Hour() + so

	offsets := []int{-11, -10, -9, 8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

	for _, v := range offsets {
		if (UTCHour+v)%24 == hour {
			offset = v
		}
	}

	return
}

// GetBOD beginning of day
// It parses the given time and sets the hours, minutes and seconds to 0
func GetBOD(t time.Time) time.Time {
	year, month, day := t.Date()
	return time.Date(year, month, day, 0, 0, 0, 0, t.Location())
}
