package registry

import (
	"encoding/hex"
	"strings"
)

const (
	maxTenantLen = 64
	maxNameLen   = 1024
	DigestLen    = 64
	MaxChunkSize = 1 << 20
)

// ValidateDigest enforces a lowercase 64-hex-char SHA-256 digest.
func ValidateDigest(digest string) error {
	if len(digest) != DigestLen {
		return newError(CodeInvalidArgument, "sha256 must be 64 lowercase hexadecimal characters")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return newError(CodeInvalidArgument, "sha256 must be valid hexadecimal")
	}
	for i := 0; i < len(digest); i++ {
		c := digest[i]
		if (c >= 'A' && c <= 'F') || !isLowerHex(c) {
			return newError(CodeInvalidArgument, "sha256 must be lowercase hexadecimal")
		}
	}
	return nil
}

func isLowerHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// ValidateTenant enforces a safe, non-traversable tenant identifier.
func ValidateTenant(tenant string) error {
	if len(tenant) == 0 || len(tenant) > maxTenantLen {
		return newError(CodeInvalidArgument, "tenant is required and must be at most 64 characters")
	}
	if tenant == "." || tenant == ".." {
		return newError(CodeInvalidArgument, "invalid tenant")
	}
	for i := 0; i < len(tenant); i++ {
		c := tenant[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return newError(CodeInvalidArgument, "tenant may only contain [A-Za-z0-9._-]")
		}
	}
	return nil
}

// ValidateObjectName enforces a slash separated relative path with no
// traversal, backslashes, control characters or empty segments.
func ValidateObjectName(name string) error {
	if len(name) == 0 || len(name) > maxNameLen {
		return newError(CodeInvalidArgument, "object_name is required and must be at most 1024 characters")
	}
	if strings.ContainsRune(name, '\\') {
		return newError(CodeInvalidArgument, "object_name must not contain backslashes")
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return newError(CodeInvalidArgument, "object_name must not contain control characters")
		}
	}
	segments := strings.Split(name, "/")
	for _, seg := range segments {
		switch {
		case seg == "":
			return newError(CodeInvalidArgument, "object_name must not contain empty path segments")
		case seg == "." || seg == "..":
			return newError(CodeInvalidArgument, "object_name must not contain . or .. segments")
		}
	}
	return nil
}
