package rabbitmqamqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("address builder test ", func() {
	// Error cases
	Describe("Error cases", func() {
		DescribeTable("should return appropriate errors",
			func(exchange, key, queue *string, expectedErr string) {
				_, err := address(exchange, key, queue, nil)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(Equal(expectedErr))
			},
			Entry("when both exchange and queue are set",
				stringPtr("my_exchange"), nil, stringPtr("my_queue"),
				"exchange and queue cannot be set together"),
			Entry("when neither exchange nor queue is set",
				nil, nil, nil,
				"exchange or queue must be set"),
		)
	})

	// Exchange-related cases
	Describe("Exchange addresses", func() {
		DescribeTable("should generate correct exchange addresses",
			func(exchange, key *string, expected string) {
				address, err := address(exchange, key, nil, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(address).To(Equal(expected))
			},
			Entry("with exchange and key",
				stringPtr("my_exchange"), stringPtr("my_key"),
				"/exchanges/my_exchange/my_key"),
			Entry("with exchange only",
				stringPtr("my_exchange"), nil,
				"/exchanges/my_exchange"),
			Entry("with special characters",
				stringPtr("my_ exchange/()"), stringPtr("my_key "),
				"/exchanges/my_%20exchange%2F%28%29/my_key%20"),
		)
	})

	// Queue-related cases
	Describe("Queue addresses", func() {
		It("should generate correct queue address with special characters", func() {
			queue := "my_queue>"
			address, err := address(nil, nil, &queue, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(address).To(Equal("/queues/my_queue%3E"))
		})

		It("should generate correct purge queue address", func() {
			queue := "my_queue"
			address, err := purgeQueueAddress(&queue)
			Expect(err).NotTo(HaveOccurred())
			Expect(address).To(Equal("/queues/my_queue/messages"))
		})
	})
})

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
