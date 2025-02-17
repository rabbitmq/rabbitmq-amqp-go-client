package rabbitmqamqp

import (
	"fmt"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Converters", func() {
	It("Converts from number", func() {
		Expect(CapacityBytes(100)).To(Equal(int64(100)))
		Expect(CapacityKB(1)).To(Equal(int64(1000)))
		Expect(CapacityMB(1)).To(Equal(int64(1000 * 1000)))
		Expect(CapacityGB(1)).To(Equal(int64(1000 * 1000 * 1000)))
		Expect(CapacityTB(1)).To(Equal(int64(1000 * 1000 * 1000 * 1000)))
	})

	It("Converts from string", func() {
		v, err := CapacityFrom("1KB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000)))

		v, err = CapacityFrom("1MB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000 * 1000)))

		v, err = CapacityFrom("1GB")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000 * 1000 * 1000)))

		v, err = CapacityFrom("1tb")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(int64(1000 * 1000 * 1000 * 1000)))
	})

	It("Converts from string logError", func() {
		v, err := CapacityFrom("10LL")
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid unit size format"))

		v, err = CapacityFrom("aGB")
		Expect(fmt.Sprintf("%s", err)).
			To(ContainSubstring("Invalid number format"))

		v, err = CapacityFrom("")
		Expect(v).To(Equal(int64(0)))
		Expect(err).To(BeNil())

		v, err = CapacityFrom("0")
		Expect(v).To(Equal(int64(0)))
		Expect(err).To(BeNil())
	})
})
