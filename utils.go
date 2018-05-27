package bunchs3

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	minio "github.com/minio/minio-go"
)

func init() {
	rand.Seed(time.Now().UnixNano())

	os.MkdirAll(tmpDir, os.ModePerm)
	os.MkdirAll(storageDir, os.ModePerm)
}

func md5sum(c []byte) string {
	h := md5.Sum(c)
	return hex.EncodeToString(h[:])
}

var letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func saveContentToLocal(name string, content []byte) (string, error) {
	tmpPath := filepath.Join(tmpDir, randString(16))
	path := filepath.Join(storageDir, name)

	err := ioutil.WriteFile(tmpPath, content, 0644)
	if err != nil {
		return "", err
	}
	err = os.Rename(tmpPath, path)
	if err != nil {
		return "", err
	}
	return path, nil
}

func saveContentToS3(clnt *minio.Client, bucket string, name string, content []byte) error {
	r := bytes.NewReader(content)
	n, err := clnt.PutObject(bucket, name, r, int64(len(content)), minio.PutObjectOptions{})
	if err != nil {
		return err
	}
	if int(n) != len(content) {
		return Error(fmt.Sprintf("content length is %d, but only %d uploaded", len(content), n))
	}
	return nil
}

func downloadFromS3ToBytes(clnt *minio.Client, bucket string, name string) ([]byte, error) {
	r, err := clnt.GetObject(bucket, name, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer r.Close()

	buf := new(bytes.Buffer)
	buf.ReadFrom(r)
	return buf.Bytes(), nil
}
