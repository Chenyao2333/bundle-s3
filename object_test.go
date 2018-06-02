package bundles3

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func Test_Object(t *testing.T) {
	bs3Cfg := genBS3Cfg(64 * 1024)
	bs3, _ := NewBundleS3(*bs3Cfg)

	// 233 KB
	s := []byte(randString(233 * 1024))
	name := "hello.txt"

	p, _ := saveContentToLocal(name, s)
	t.Log(p)
	o, err := NewObjectFromLocalFile(p, name)
	if err != nil {
		t.Fatal(err)
	}

	err = o.Upload(bs3)
	if err != nil {
		t.Fatal(err)
	}

	os.Remove(p)
	o1, err := NewObjectFromBundleS3(name, bs3)
	if err != nil {
		t.Fatal(err)
	}
	s1, err := ioutil.ReadFile(o1.Path())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s, s1) {
		t.Fatalf("Object's content is changed.")
	}
}
