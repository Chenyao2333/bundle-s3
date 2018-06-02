package bundles3

import (
	"github.com/klauspost/reedsolomon"
	"github.com/minio/minio-go"
)

// BundleS3 implements object-storage with the bunch of s3 buckets as backend.
type BundleS3 struct {
	cfg   Config
	clnts []*minio.Client
	enc   reedsolomon.Encoder
}

// S3Config is config for s3 service
type S3Config struct {
	endpoint string
	ak       string
	sk       string
	bucket   string
	rank     int
}

// Config is config for BundleS3
type Config struct {
	s3cfgs       []S3Config
	dataShards   int
	parityShards int
	chunkSize    int64
}

// Error is the error type for bundles3
type Error string

func (e Error) Error() string {
	return string(e)
}

var errNonImplemented = Error("Non implemented")
var errS3cfgsLen = Error("s3cfgs' length should equal to dataShards + parityShards")
var errS3cfgsRank = Error("S3Config's rank should equal to its index in array, it just used to emphasize the importance of order.")
var errShardsNum = Error("Shards shourld greater than 0.")

var tmpDir = "/tmp/bundles3_tmp"
var storageDir = "/tmp/bundles3_storage"

// NewConfig returns a pointer of Config instance.
// s3cfgs is the array of s3 backends, the order is IMPORTANT.
// S3Config's rank should equal to its index in array,
// it just used to emphasize the importance of order.
// It needs dataShards + parityShards = len(s3cfgs).
func NewConfig(s3cfgs []S3Config, dataShards int, parityShards int, chunkSize int64) (*Config, error) {
	if len(s3cfgs) != dataShards+parityShards {
		return nil, errS3cfgsLen
	}
	if dataShards <= 0 || parityShards <= 0 {
		return nil, errShardsNum
	}
	for i := 0; i < len(s3cfgs); i++ {
		if s3cfgs[i].rank != i {
			return nil, errS3cfgsRank
		}
	}

	cfg := &Config{s3cfgs, dataShards, parityShards, chunkSize}
	return cfg, nil
}

// NewBundleS3 returns a pointer of BundleS3 instance.
func NewBundleS3(cfg Config) (*BundleS3, error) {
	bs3 := &BundleS3{}
	bs3.cfg = cfg
	if len(cfg.s3cfgs) != cfg.dataShards+cfg.parityShards {
		return nil, errS3cfgsLen
	}
	var err error
	bs3.enc, err = reedsolomon.New(cfg.dataShards, cfg.parityShards)
	if err != nil {
		return nil, err
	}
	bs3.clnts = make([]*minio.Client, len(cfg.s3cfgs))
	for i, s3cfg := range cfg.s3cfgs {
		clnt, err := minio.New(s3cfg.endpoint, s3cfg.ak, s3cfg.sk, false)
		if err != nil {
			return nil, err
		}
		bs3.clnts[i] = clnt
	}

	return bs3, nil
}

func (bs3 *BundleS3) Put(name string, o *Object) error {
	return o.Upload(bs3)
}

func (bs3 *BundleS3) Get(name string) (*Object, error) {
	return NewObjectFromBundleS3(name, bs3)
}

// List isn't implemented.
func (bs3 *BundleS3) List(name string) ([]string, error) {
	return nil, errNonImplemented
}

// Delete isn't implemented.
func (bs3 *BundleS3) Delete(name string) error {
	return errNonImplemented
}
