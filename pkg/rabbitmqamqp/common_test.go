package rabbitmqamqp

import (
	"crypto/md5"
	"encoding/base64"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("generateName", func() {

	// Security: incorrect MD5 usage (Vuln 3, SECURITY_REPORT.md).
	// The previous implementation called md5obj.Sum(uuidBytes), which appends
	// MD5("") to uuidBytes instead of hashing them. Every generated name shared
	// a predictable 16-byte suffix equal to MD5(""), and the raw UUID string was
	// encoded verbatim in the output.

	It("produces unique names on successive calls", func() {
		Expect(generateName("prefix-")).NotTo(Equal(generateName("prefix-")))
	})

	It("includes the given prefix", func() {
		Expect(generateName("client.gen-")).To(HavePrefix("client.gen-"))
	})

	It("does not embed the MD5-of-empty-string constant in the output", func() {
		// MD5("") = d41d8cd98f00b204e9800998ecf8427e
		// Before the fix the last 16 decoded bytes were always this value.
		emptyMD5 := md5.New().Sum(nil)

		name := generateName("pfx-")
		encoded := strings.TrimPrefix(name, "pfx-")
		// Restore padding and substitutions made by generateName.
		for len(encoded)%4 != 0 {
			encoded += "="
		}
		encoded = strings.ReplaceAll(encoded, "-", "+")
		encoded = strings.ReplaceAll(encoded, "_", "/")

		decoded, err := base64.StdEncoding.DecodeString(encoded)
		Expect(err).NotTo(HaveOccurred())

		// After the fix the digest is exactly 16 bytes (MD5 output size).
		Expect(decoded).To(HaveLen(16))

		// The digest must NOT equal MD5("").
		Expect(decoded).NotTo(Equal(emptyMD5),
			"digest must be MD5 of the UUID bytes, not MD5 of empty input")
	})

	It("produces a base64url-safe name with no +, /, or = characters", func() {
		name := generateName("pfx-")
		body := strings.TrimPrefix(name, "pfx-")
		Expect(body).NotTo(ContainSubstring("+"))
		Expect(body).NotTo(ContainSubstring("/"))
		Expect(body).NotTo(ContainSubstring("="))
	})
})
