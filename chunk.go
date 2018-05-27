package bunchs3

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

type chunk struct {
	md5  string
	size int64
	path string
}

func newChunk(content []byte) (*chunk, error) {
	c := &chunk{}
	c.md5 = md5sum(content)
	c.size = int64(len(content))

	var err error
	c.path, err = saveContentToLocal(c.md5, content)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func newChunkFromMD5(md5 string) (*chunk, error) {
	c, err := newChunkFromLocal(md5)
	if err == nil {
		return c, nil
	}

	c, err = newChunkFromBunchS3(md5)
	if err == nil {
		return c, nil
	}

	return nil, err
}

func newChunkFromLocal(md5 string) (*chunk, error) {
	c := &chunk{}
	c.md5 = md5
	c.path = filepath.Join(storageDir, c.md5)
	fi, err := os.Stat(c.path)
	// doesn't in local
	if err != nil {
		return nil, err
	}
	c.size = fi.Size()
	return c, nil
}

func newChunkFromBunchS3(md5 string) (*chunk, error) {
	// TODO
	return nil, errNonImplemented
}

func (c *chunk) uploadToBunchS3() error {
	// TODO
	return errNonImplemented
}

func (c *chunk) content() ([]byte, error) {
	ct, err := ioutil.ReadFile(c.path)
	if err != nil {
		return nil, err
	}
	return ct, nil
}
