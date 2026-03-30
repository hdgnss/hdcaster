package security

import (
	"crypto/rand"
	"crypto/sha512"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	passwordHashPrefix     = "hdcaster-sha512"
	passwordHashIterations = 200000
	passwordSaltSize       = 16
)

var ErrPasswordHashFormat = errors.New("security: invalid password hash format")

func HashPasswordSHA512Salted(password string) (string, error) {
	salt := make([]byte, passwordSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("security: generate salt: %w", err)
	}
	digest := derivePasswordDigest(password, salt, passwordHashIterations)
	return encodePasswordHash(passwordHashIterations, salt, digest[:]), nil
}

func VerifyPasswordSHA512Salted(password, encoded string) (bool, error) {
	iterations, salt, expected, err := decodePasswordHash(encoded)
	if err != nil {
		return false, err
	}
	actual := derivePasswordDigest(password, salt, iterations)
	if subtle.ConstantTimeCompare(actual[:], expected) != 1 {
		return false, nil
	}
	return true, nil
}

func HashPassword(password string) (string, error) {
	return HashPasswordSHA512Salted(password)
}

func CheckPassword(encoded, password string) bool {
	ok, err := VerifyPasswordSHA512Salted(password, encoded)
	return err == nil && ok
}

func encodePasswordHash(iterations int, salt, digest []byte) string {
	return strings.Join([]string{
		passwordHashPrefix,
		strconv.Itoa(iterations),
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(digest),
	}, "$")
}

func decodePasswordHash(encoded string) (int, []byte, []byte, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 4 || parts[0] != passwordHashPrefix {
		return 0, nil, nil, ErrPasswordHashFormat
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations <= 0 {
		return 0, nil, nil, ErrPasswordHashFormat
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil || len(salt) == 0 {
		return 0, nil, nil, ErrPasswordHashFormat
	}
	digest, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil || len(digest) != sha512.Size {
		return 0, nil, nil, ErrPasswordHashFormat
	}
	return iterations, salt, digest, nil
}

func derivePasswordDigest(password string, salt []byte, iterations int) [sha512.Size]byte {
	var digest [sha512.Size]byte
	material := make([]byte, 0, len(salt)+len(password)+sha512.Size)
	material = append(material, salt...)
	material = append(material, password...)
	sum := sha512.Sum512(material)
	digest = sum
	for i := 1; i < iterations; i++ {
		material = material[:0]
		material = append(material, digest[:]...)
		material = append(material, salt...)
		material = append(material, password...)
		sum = sha512.Sum512(material)
		digest = sum
	}
	return digest
}
