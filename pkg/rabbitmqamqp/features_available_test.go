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

	It("ConsumerOptions validate for single active consumer state notification", func() {
		nop := func(*Consumer, bool) {}

		Expect((&ConsumerOptions{
			SingleActiveConsumerStateChanged: nop,
		}).validate(&featuresAvailable{is43rMore: false})).To(MatchError(ContainSubstring("4.3")))

		Expect((&ConsumerOptions{
			SettleStrategy:                   DirectReplyTo,
			SingleActiveConsumerStateChanged: nop,
		}).validate(&featuresAvailable{is43rMore: true, is42rMore: true})).To(MatchError(ContainSubstring("DirectReplyTo")))

		Expect((&ConsumerOptions{
			SingleActiveConsumerStateChanged: nop,
		}).validate(&featuresAvailable{is43rMore: true})).To(BeNil())
	})

	It("extractMessageRejectedError returns nil for non-rejected states", func() {
		Expect(extractMessageRejectedError(&StateAccepted{})).To(BeNil())
		Expect(extractMessageRejectedError(&StateReleased{})).To(BeNil())
		Expect(extractMessageRejectedError(&StateModified{})).To(BeNil())
	})

	It("extractMessageRejectedError returns nil for StateRejected without Error", func() {
		Expect(extractMessageRejectedError(&StateRejected{})).To(BeNil())
	})

	It("extractMessageRejectedError extracts RejectedBy and Reason from StateRejected.Error", func() {
		state := &StateRejected{
			Error: &amqp.Error{
				Condition:   "amqp:resource-limit-exceeded",
				Description: "maximum length reached",
				Info:        map[string]any{"queue": "my-queue"},
			},
		}
		result := extractMessageRejectedError(state)
		Expect(result).NotTo(BeNil())
		Expect(result.RejectedBy).To(Equal("my-queue"))
		Expect(result.Reason).To(Equal("maximum length reached"))
		Expect(result.Error()).To(ContainSubstring("my-queue"))
		Expect(result.Error()).To(ContainSubstring("maximum length reached"))
	})

	It("extractMessageRejectedError handles missing rejected-by key gracefully", func() {
		state := &StateRejected{
			Error: &amqp.Error{
				Description: "queue unavailable",
			},
		}
		result := extractMessageRejectedError(state)
		Expect(result).NotTo(BeNil())
		Expect(result.RejectedBy).To(BeEmpty())
		Expect(result.Reason).To(Equal("queue unavailable"))
		Expect(result.Error()).To(ContainSubstring("queue unavailable"))
	})

	// Security: unchecked type assertions on server-supplied connection properties.
	// A rogue or MITM broker can send non-string values for "version"/"product",
	// which previously caused a runtime panic at dial time (Vuln 2, SECURITY_REPORT.md).

	It("ParseProperties returns error when 'product' key is absent", func() {
		fa := newFeaturesAvailable()
		err := fa.ParseProperties(map[string]any{
			"version": "4.0.0",
			// "product" intentionally absent
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(fa.isRabbitMQ).To(BeFalse())
		Expect(fa.isTanzu).To(BeFalse())
		Expect(fa.is4OrMore).To(BeTrue())
	})

	It("ParseProperties returns error when 'version' is not a string", func() {
		fa := newFeaturesAvailable()
		err := fa.ParseProperties(map[string]any{
			"version": 400,
			"product": "RabbitMQ",
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("string"))
	})

	It("ParseProperties returns error when 'product' is not a string", func() {
		fa := newFeaturesAvailable()
		err := fa.ParseProperties(map[string]any{
			"version": "4.0.0",
			"product": 99,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(fa.isRabbitMQ).To(BeFalse())
		Expect(fa.is4OrMore).To(BeTrue())
	})

	It("ParseProperties succeeds and marks isRabbitMQ false for a non-standard server", func() {
		fa := newFeaturesAvailable()
		err := fa.ParseProperties(map[string]any{
			"version": "4.1.0",
			"product": "SomeOtherBroker",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(fa.isRabbitMQ).To(BeFalse())
		Expect(fa.is41OrMore).To(BeTrue())
	})

})
