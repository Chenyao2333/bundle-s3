package bunchs3

import (
	"bytes"
	"testing"
)

func Test_Content(t *testing.T) {
	s := []byte("world")
	c1, _ := newChunk(s)
	c2, _ := newChunkFromMD5(c1.md5)
	s2, _ := c2.content()
	if !bytes.Equal(s, s2) {
		t.Fatalf("Chunk's content is changed.")
	}
}
