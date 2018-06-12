package bundles3

import (
	"encoding/json"
	"io"
	"os"
)

type Object struct {
	name       string
	local_path string
}

type meta struct {
	Size      int64    `json:"size"`
	ChunksMD5 []string `json:"chunksMD5"`
}

func NewObjectFromLocalFile(path string, name string) (*Object, error) {
	if !fileExists(path) {
		return nil, Error("Non exists such file.")
	}
	if len(name) < 1 {
		return nil, Error("Empty name is not allowed.")
	}

	o := &Object{}
	o.name = name
	o.local_path = path
	return o, nil
}

func NewObjectFromContent(c []byte, name string) (*Object, error) {
	if len(name) < 1 {
		return nil, Error("Empty name is not allowed.")
	}
	path, err := saveContentToLocal(name, c)
	if err != nil {
		return nil, err
	}

	o := &Object{}
	o.name = name
	o.local_path = path
	return o, nil
}

func (o *Object) Path() string {
	return o.local_path
}

func (o *Object) splitToChunks(sizePerChunk int64) ([]*chunk, error) {
	totalSize, err := filseSize(o.local_path)
	if err != nil {
		return nil, err
	}

	chunksNum := totalSize / sizePerChunk
	if totalSize%sizePerChunk > 0 {
		chunksNum++
	}
	chunks := make([]*chunk, chunksNum)

	f, err := os.Open(o.local_path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	for i := range chunks {
		buf := make([]byte, sizePerChunk)
		n, err := f.Read(buf)
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
		}
		buf = buf[:n]

		chunks[i], err = newChunk(buf)
		if err != nil {
			return nil, err
		}
	}

	return chunks, nil
}

func (o *Object) genMeta(cs []*chunk) meta {
	m := meta{}
	m.Size = 0
	m.ChunksMD5 = make([]string, len(cs))
	for i, c := range cs {
		m.Size += c.size
		m.ChunksMD5[i] = c.md5
	}
	return m
}

func downloadMeta(bs3 *BundleS3, name string) (meta, error) {
	name = "index/" + name
	for i := range bs3.clnts {
		byteM, err := downloadFromS3ToBytes(bs3.clnts[i], bs3.cfg.s3cfgs[i].Bucket, name)
		if err == nil {
			var m meta
			err = json.Unmarshal(byteM, &m)
			return m, err
		}
	}
	return meta{}, Error("Not found.")
}

func (o *Object) Upload(bs3 *BundleS3) error {
	chunks, err := o.splitToChunks(bs3.cfg.chunkSize)
	if err != nil {
		return err
	}
	for _, c := range chunks {
		err := c.upload(bs3)
		if err != nil {
			return err
		}
	}

	indexFileName := "index/" + o.name
	m := o.genMeta(chunks)
	byteM, err := json.Marshal(m)
	if err != nil {
		return err
	}

	ch := make(chan error, bs3.cfg.dataShards+bs3.cfg.parityShards)
	f := func(i int) {
		err := saveContentToS3(bs3.clnts[i], bs3.cfg.s3cfgs[i].Bucket, indexFileName, byteM)
		ch <- err
	}
	for i := range bs3.clnts {
		go f(i)
	}

	succCount := 0
	for range bs3.clnts {
		err := <-ch
		if err == nil {
			succCount++
		}
	}

	// downgrade writing
	if succCount >= 1 {
		return nil
	}
	return Error("No index file saved successfully")
}

func NewObjectFromBundleS3(name string, bs3 *BundleS3) (*Object, error) {
	m, err := downloadMeta(bs3, name)
	if err != nil {
		return nil, err
	}

	chunks := make([]*chunk, len(m.ChunksMD5))
	for i, md5 := range m.ChunksMD5 {
		chunks[i], err = newChunkFromMD5(md5, bs3)
		if err != nil {
			return nil, err
		}
	}

	p, err := mergeChunkToLocalFile(chunks, name)
	if err != nil {
		return nil, err
	}
	return NewObjectFromLocalFile(p, name)
}
