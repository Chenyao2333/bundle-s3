package bunchs3

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"testing"
)

func Test_randString(t *testing.T) {
	n := 5
	s := randString(n)
	t.Logf("Generated string: %s", s)
	if len(s) != n {
		t.FailNow()
	}
}

func Test_md5sum(t *testing.T) {
	s := []byte("hello")
	md5 := "5d41402abc4b2a76b9719d911017c592"
	if md5sum(s) != md5 {
		t.FailNow()
	}
}

func Test_saveContentToLocal(t *testing.T) {
	s := []byte("world")
	_, err := saveContentToLocal("hello.txt", s)
	if err != nil {
		t.Fatal(err)
	}

	r, err := ioutil.ReadFile(filepath.Join(storageDir, "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(s, r) {
		t.Fatalf("Saved file s changed")
	}
}
