package bunchs3

import (
	"crypto/md5"
	"encoding/hex"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"
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
