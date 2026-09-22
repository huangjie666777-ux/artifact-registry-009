package httpapi

import (
	"io"
	"os"

	"github.com/labstack/echo/v4"

	"artifact-registry/internal/registry"
)

// sectionFile wraps an opened blob for range/full streaming.
type sectionFile struct {
	f *os.File
}

func openSection(store *registry.Store, c echo.Context, digest string) (*sectionFile, error) {
	f, _, err := store.BlobReader(c.Request().Context(), digest)
	if err != nil {
		return nil, err
	}
	return &sectionFile{f: f}, nil
}

func (s *sectionFile) Close() error { return s.f.Close() }

func (s *sectionFile) writeRange(w io.Writer, offset, length int64) (int64, error) {
	remaining := length
	buf := make([]byte, 64*1024)
	var written int64
	for remaining > 0 {
		want := int64(len(buf))
		if want > remaining {
			want = remaining
		}
		n, err := s.f.ReadAt(buf[:want], offset+written)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return written, werr
			}
			written += int64(n)
			remaining -= int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

func (s *sectionFile) writeAll(w io.Writer, size int64) error {
	n, err := s.writeRange(w, 0, size)
	if err != nil {
		return err
	}
	if n != size {
		return io.ErrUnexpectedEOF
	}
	return nil
}
