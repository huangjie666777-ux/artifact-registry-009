package registry

import (
	"crypto/sha256"
	"encoding/hex"
)

type streamingHash struct {
	h interface {
		Write(p []byte) (int, error)
		Sum(b []byte) []byte
	}
}

func newSHA256() *streamingHash {
	return &streamingHash{h: sha256.New()}
}

func (s *streamingHash) write(p []byte) {
	_, _ = s.h.Write(p)
}

func (s *streamingHash) hex() string {
	return hex.EncodeToString(s.h.Sum(nil))
}
