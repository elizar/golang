package utils

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"time"

	as3 "github.com/apex/go-apex/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3GetFromKey gets the s3 object from the given key,
// bucket name and an optional decompress param.
func S3GetFromKey(key string, bucket string, decompress bool) ([]byte, error) {
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	params := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	item, err := svc.GetObject(params)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(item.Body)

	if decompress {
		rc, err := DecompressBas64(body)
		if err != nil {
			return nil, err
		}

		ba, err := ioutil.ReadAll(rc)
		if err != nil {
			return nil, err
		}

		return ba, nil
	}

	return body, nil
}

// S3GetFromEvent gets the s3 object with the provided
// event ( json.RawMessage ), bucket name and an optional decompress param.
func S3GetFromEvent(event json.RawMessage, bucket string, decompress bool) (body []byte, err error) {
	body, _, err = S3GetFromEventWithKey(event, bucket, decompress)
	return
}

func S3GetFromEventWithKey(event json.RawMessage, bucket string, decompress bool) (body []byte, key string, err error) {
	key, err = S3GetKeyFromEvent(event)
	if err != nil {
		return
	}

	body, err = S3GetFromKey(key, bucket, decompress)
	return
}

// S3GetKeyFromEvent gets the key from the gevent event json payload
func S3GetKeyFromEvent(event json.RawMessage) (key string, err error) {
	var evt as3.Event

	err = json.Unmarshal(event, &evt)
	if err != nil {
		return
	}

	key = evt.Records[0].S3.Object.Key
	return
}

// S3Upload uploads the given list of User to S3 with
// a given bucket, key
func S3Upload(item interface{}, bucket string, key string, compress bool) (err error) {
	var data string
	ctype := "text/plain"

	if compress {
		data, err = Base64Compress(item)
		if err != nil {
			return
		}
	} else {
		var b []byte

		b, err = json.Marshal(item)
		if err != nil {
			return
		}

		data = string(b)
		ctype = "application/json"
	}

	err = S3UploadWithType([]byte(data), bucket, key, ctype)
	return
}

// S3UploadWithType uploads data to s3 with a given bucket, key and content-type
func S3UploadWithType(data []byte, bucket string, key string, ctype string) (err error) {
	svc := s3.New(session.New(), &aws.Config{Region: aws.String("us-east-1")})

	params := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),                    // Required
		Key:         aws.String(key),                       // Required
		Expires:     aws.Time(time.Now().AddDate(0, 0, 1)), // 1 day from now
		Body:        bytes.NewReader(data),
		ContentType: aws.String(ctype),
	}

	_, err = svc.PutObject(params)
	return
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
	_, err = w.Write(d)
	if err != nil {
		return "", err
	}
	err = w.Close()
	if err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b.Bytes()), nil
}

// DecompressBas64 decompresses base64 encoded and zlib compressed
// data.
func DecompressBas64(data []byte) (io.ReadCloser, error) {
	d, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil, err
	}

	body, err := Decompress(d, "")
	return body, err
}

// Decompress decompresses data from the given compression.
// which defaults to zlib.
func Decompress(data []byte, compression string) (io.ReadCloser, error) {
	b := bytes.NewReader(data)
	var r io.ReadCloser
	var err error

	switch compression {
	case "gzip":
		r, err = gzip.NewReader(b)
	default:
		r, err = zlib.NewReader(b)
	}
	_ = r.Close()

	return r, err
}

// LogIt logs the given message string to Stderr instead of
// Stdout. Because when you run your GO code in lambda using
// node.js shim, the Stdout is used by node.js to communicate
// with your GO program.
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

	offsets := []int{-11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}

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

// Round a simple floating point round
func Round(f float64) float64 {
	if f < 0 {
		return math.Ceil(f - 0.5)
	}
	return math.Floor(f + 0.5)
}

// RoundPlus round with a specific precision
func RoundPlus(f float64, places int) float64 {
	shift := math.Pow(10, float64(places))
	return Round(f*shift) / shift
}

// Pair a data structure to hold a key/value Pair.
type Pair struct {
	Key   string
	Value int
}

// PairList Pair list
type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

// SortMapByValue sorts a givem map with string as key and int as value
func SortMapByValue(m map[string]int, limit int, reverse bool) PairList {
	p := make(PairList, len(m))
	i := 0
	for k, v := range m {
		p[i] = Pair{k, v}
		i++
	}

	if reverse {
		sort.Sort(sort.Reverse(p))
	} else {
		sort.Sort(p)
	}

	if limit > 0 && limit < len(m) {
		return p[:limit]
	}
	return p
}

// Transact handles db transaction with proper rollback
// and commit execution
func Transact(db *sql.DB, txFunc func(*sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			switch p := p.(type) {
			case error:
				err = p
			default:
				err = fmt.Errorf("%s", p)
			}
		}
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()
	return txFunc(tx)
}
