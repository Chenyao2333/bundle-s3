package main

import (
	"log"
	"os"

	"github.com/minio/minio-go"
)

func main() {
	// Note: YOUR-ACCESSKEYID, YOUR-SECRETACCESSKEY, my-bucketname, my-objectname and
	// my-testfile are dummy values, please replace them with original values.

	// Requests are always secure (HTTPS) by default. Set secure=false to enable insecure (HTTP) access.
	// This boolean value is the last argument for New().

	// New returns an Amazon S3 compatible client object. API compatibility (v2 or v4) is automatically
	// determined based on the Endpoint value.
	s3Client, err := minio.New("storage.googleapis.com", "GOOGRAQOLAYSQGMH6LT66OJ3", "QXYEyoDqBmOhSCVSrs+HYUwSoDz9msola52LMgfX", true)
	//s3Client.overrideSignerType = credentials.SignatureV2
	if err != nil {
		log.Fatalln(err)
	}

	object, err := os.Open("/tmp/bunchs3_storage/hello.txt")
	if err != nil {
		log.Fatalln(err)
	}
	defer object.Close()
	objectStat, err := object.Stat()
	if err != nil {
		log.Fatalln(err)
	}

	n, err := s3Client.PutObject("bunchs3-0", "hello.txt", object, objectStat.Size(), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
}
