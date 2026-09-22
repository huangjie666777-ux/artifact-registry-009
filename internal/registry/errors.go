package registry

import "fmt"

// Stable error codes used across the registry package and rendered by the HTTP layer.
const (
	CodeInvalidArgument = "invalid_argument"
	CodeNotFound        = "not_found"
	CodeChunkConflict   = "chunk_conflict"
	CodeUploadNotOpen   = "upload_not_open"
	CodeIncomplete      = "upload_incomplete"
	CodeDigestMismatch  = "digest_mismatch"
	CodePayloadTooLarge = "payload_too_large"
	CodeRangeInvalid    = "invalid_range"
	CodeRangeNotSatisf  = "range_not_satisfiable"
	CodeInternal        = "internal_error"
)

// Error is a stable, machine-readable error carried across package boundaries.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

func newError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

func wrapError(code, message string, cause error) *Error {
	return &Error{Code: code, Message: message, cause: cause}
}

// AsError extracts a registry *Error from err, if present.
func AsError(err error) (*Error, bool) {
	var target *Error
	for err != nil {
		if e, ok := err.(*Error); ok {
			return e, true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		err = u.Unwrap()
	}
	return target, false
}
