package bundles3

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/minio/minio-go"
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

func newChunkFromMD5(md5 string, bs3 *BundleS3) (*chunk, error) {
	c, err := newChunkFromLocal(md5)
	if err == nil {
		return c, nil
	}

	c, err = newChunkFromBundleS3(md5, bs3)
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

func newChunkFromBundleS3(md5 string, bs3 *BundleS3) (*chunk, error) {
	c := &chunk{}
	c.md5 = md5

	data := make([][]byte, bs3.cfg.dataShards+bs3.cfg.parityShards)

	ch := make(chan error, len(data))

	r := func(i int) {
		times := 0
		var err error
		for times < 3 {
			times++
			d, err := downloadFromS3ToBytes(bs3.clnts[i], bs3.cfg.s3cfgs[i].bucket, c.shardName(i))
			if err != nil {
				time.Sleep(2 * time.Second)
			} else {
				c.size = int64(binary.LittleEndian.Uint64(d[:8]))
				data[i] = d[8:]
				ch <- nil
				return
			}
		}
		ch <- err
	}

	for i := range data {
		go r(i)
	}
	errCount := 0
	for i := 0; i < len(data); i++ {
		err := <-ch
		if err != nil {
			errCount++
		}
		// TODO: break after enough data
	}

	if errCount > bs3.cfg.parityShards {
		return nil, Error(fmt.Sprintf("Can't recorver since there are %d errors.", errCount))
	}
	err := bs3.enc.Reconstruct(data)
	if err != nil {
		return nil, err
	}

	// TODO: direct write to files
	buf := new(bytes.Buffer)
	err = bs3.enc.Join(buf, data, int(c.size))
	if err != nil {
		return nil, err
	}

	c.path, err = saveContentToLocal(c.md5, buf.Bytes())
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *chunk) shardName(i int) string {
	return "shards/" + c.md5 + "_" + strconv.Itoa(i)
}

func (c *chunk) upload(bs3 *BundleS3) error {
	content, err := ioutil.ReadFile(c.path)
	if err != nil {
		return err
	}

	// If len(content) = 0, reedsolomon doesn't work.
	// It doesn't mattar existing the garbage string,
	// beacause it will be truncated since recorver.
	if len(content) == 0 {
		content = []byte("dummy")
	}

	data, err := bs3.enc.Split(content)
	if err != nil {
		return err
	}
	err = bs3.enc.Encode(data)
	if err != nil {
		return err
	}

	ch := make(chan error, len(data))

	f := func(clnt *minio.Client, bucket string, name string, content []byte) {
		times := 0
		var err error
		for times < 3 {
			times++
			err = saveContentToS3(clnt, bucket, name, content)
			if err != nil {
				time.Sleep(2 * time.Second)
			} else {
				ch <- nil
				return
			}
		}
		ch <- err
	}

	hdrSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(hdrSize, uint64(c.size))

	for i, d := range data {
		go f(bs3.clnts[i], bs3.cfg.s3cfgs[i].bucket, c.shardName(i), append(hdrSize, d...))
	}

	for i := 0; i < len(data); i++ {
		err := <-ch
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *chunk) content() ([]byte, error) {
	ct, err := ioutil.ReadFile(c.path)
	if err != nil {
		return nil, err
	}
	return ct, nil
}
