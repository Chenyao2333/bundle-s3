package bundles3

import (
	"bytes"
	"os"
	"testing"
)

func genBS3Cfg(chunkSize int64) *Config {
	ak := "GOOGRAQOLAYSQGMH6LT66OJ3"
	sk := "QXYEyoDqBmOhSCVSrs+HYUwSoDz9msola52LMgfX"

	bs3Cfg, _ := NewConfig([]S3Config{
		S3Config{"storage.googleapis.com", ak, sk, "bundles3-0", 0},
		S3Config{"storage.googleapis.com", ak, sk, "bundles3-1", 1},
		S3Config{"storage.googleapis.com", ak, sk, "bundles3-2", 2},
	}, 2, 1, chunkSize)

	return bs3Cfg
}

func Test_chunk(t *testing.T) {
	bs3Cfg := genBS3Cfg(64 * 1024)
	bs3, _ := NewBundleS3(*bs3Cfg)

	s := []byte("world")
	c1, _ := newChunk(s)
	c2, _ := newChunkFromMD5(c1.md5, bs3)
	s2, _ := c2.content()
	if !bytes.Equal(s, s2) {
		t.Fatalf("Chunk's content is changed.")
	}

	err := c2.upload(bs3)
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(c2.path)

	c3, err := newChunkFromMD5(c1.md5, bs3)
	if err != nil {
		t.Fatal(err)
	}
	s3, _ := c3.content()
	if !bytes.Equal(s, s3) {
		t.Fatalf("Chunk's content is changed after download.")
	}
}
