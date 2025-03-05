package rabbitmqamqp

import (
	"fmt"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Available Features", func() {

	It("Parse Version", func() {
		v, err := parseVersion("1.2.3")
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 1, Minor: 2, Patch: 3}))

		_, err = parseVersion("1.2")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid version format: 1.2"))

		_, err = parseVersion("error.3.3")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid major version: error"))

		_, err = parseVersion("1.error.3")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid minor version: error"))

		_, err = parseVersion("1.2.error")
		Expect(err).To(HaveOccurred())
		Expect(fmt.Sprintf("%s", err)).To(ContainSubstring("invalid patch version: error"))

		v, err = parseVersion(extractVersion("3.12.1-rc1"))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 3, Minor: 12, Patch: 1}))

		v, err = parseVersion(extractVersion("3.13.1-alpha.234"))
		Expect(err).NotTo(HaveOccurred())
		Expect(v).To(Equal(Version{Major: 3, Minor: 13, Patch: 1}))
	})

	It("Is Version Greater Or Equal", func() {
		Expect(isVersionGreaterOrEqual("1.2.3", "1.2.3")).To(BeTrue())
		Expect(isVersionGreaterOrEqual("1.2.3", "1.2.2")).To(BeTrue())
		Expect(isVersionGreaterOrEqual("1.2.3", "1.2.4")).To(BeFalse())
		Expect(isVersionGreaterOrEqual("1.2.3", "1.3.3")).To(BeFalse())
		Expect(isVersionGreaterOrEqual("1.2.3", "2.2.3")).To(BeFalse())
		Expect(isVersionGreaterOrEqual("3.1.3-alpha.1", "2.2.3")).To(BeFalse())
		Expect(isVersionGreaterOrEqual("3.3.3-rc.1", "2.2.3")).To(BeFalse())

		Expect(isVersionGreaterOrEqual("error.3.2", "2.2.3")).To(BeFalse())
		Expect(isVersionGreaterOrEqual("4.3.2", "2.error.3")).To(BeFalse())

	})

	It("Available Features check Version", func() {
		var availableFeatures = newFeaturesAvailable()
		Expect(availableFeatures).NotTo(BeNil())
		Expect(availableFeatures.ParseProperties(map[string]any{})).NotTo(BeNil())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "3.9.0",
			"product": "RabbitMQ",
		})).To(BeNil())
		Expect(availableFeatures.is4OrMore).To(BeFalse())
		Expect(availableFeatures.is41OrMore).To(BeFalse())
		Expect(availableFeatures.isRabbitMQ).To(BeTrue())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "3.11.0",
			"product": "RabbitMQ",
		})).To(BeNil())
		Expect(availableFeatures.is4OrMore).To(BeFalse())
		Expect(availableFeatures.is41OrMore).To(BeFalse())
		Expect(availableFeatures.isRabbitMQ).To(BeTrue())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "4.0.6-rc.1",
			"product": "RabbitMQ",
		})).To(BeNil())

		Expect(availableFeatures.is4OrMore).To(BeTrue())
		Expect(availableFeatures.is41OrMore).To(BeFalse())
		Expect(availableFeatures.isRabbitMQ).To(BeTrue())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "4.1.0",
			"product": "RabbitMQ",
		})).To(BeNil())

		Expect(availableFeatures.is4OrMore).To(BeTrue())
		Expect(availableFeatures.is41OrMore).To(BeTrue())
		Expect(availableFeatures.isRabbitMQ).To(BeTrue())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "4.1.0-beta.1",
			"product": "Boh",
		})).To(BeNil())

		Expect(availableFeatures.is4OrMore).To(BeTrue())
		Expect(availableFeatures.is41OrMore).To(BeTrue())
		Expect(availableFeatures.isRabbitMQ).To(BeFalse())

		Expect(availableFeatures.ParseProperties(map[string]any{
			"version": "4.1.0-rc.8",
			"product": "rabbitmq",
		})).To(BeNil())

		Expect(availableFeatures.is4OrMore).To(BeTrue())
		Expect(availableFeatures.is41OrMore).To(BeTrue())
		Expect(availableFeatures.isRabbitMQ).To(BeTrue())
	})

	It("StreamConsumerOptions validate for RabbitMQ 4.1", func() {
		Expect((&StreamConsumerOptions{
			StreamFilterOptions: &StreamFilterOptions{
				Properties: &amqp.MessageProperties{
					MessageID: "123",
				},
			},
		}).validate(&featuresAvailable{is41OrMore: false})).To(MatchError("stream consumer with properties filter is not supported. You need RabbitMQ 4.1 or later"))

		Expect((&StreamConsumerOptions{
			StreamFilterOptions: &StreamFilterOptions{
				Properties: &amqp.MessageProperties{
					MessageID: "123",
				},
			},
		}).validate(&featuresAvailable{is41OrMore: true})).To(BeNil())
	})

})
