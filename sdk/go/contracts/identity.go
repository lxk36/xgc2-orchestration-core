package contracts

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	identifierPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]*$`)
	versionPattern    = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._+-]*$`)
	typeRefPattern    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._-]*/v[1-9][0-9]*$`)
	digestPattern     = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// ValidIdentifier reports whether value is a bounded, portable protocol
// identifier. Product-specific meaning is deliberately outside this package.
func ValidIdentifier(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && identifierPattern.MatchString(value)
}

// ValidVersion accepts portable release identities including SemVer values
// such as 1.4.0 and prerelease/build suffixes. Versions are values, not
// resource identifiers, so they may begin with a digit.
func ValidVersion(value string) bool {
	return value != "" && len(value) <= 128 && utf8.ValidString(value) &&
		strings.TrimSpace(value) == value && versionPattern.MatchString(value)
}

func ValidTypeRef(value string) bool {
	return value != "" && len(value) <= 160 && typeRefPattern.MatchString(value)
}

func ValidDigest(value string) bool {
	return digestPattern.MatchString(value)
}

func ValidOptionalDigest(value string) bool {
	return value == "" || ValidDigest(value)
}

// SecretHandle is an opaque capability identity. It intentionally has no
// JSON tags: public definitions and inputs carry secret references, never
// resolved secret values or handles.
type SecretHandle struct {
	ID string
}

func (handle SecretHandle) Valid() bool {
	return ValidIdentifier(handle.ID)
}
