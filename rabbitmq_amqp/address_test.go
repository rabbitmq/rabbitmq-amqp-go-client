package rabbitmq_amqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("address builder test ", func() {
	It("With exchange, queue and key should raise and error", func() {
		queue := "my_queue"
		exchange := "my_exchange"

		_, err := address(&exchange, nil, &queue, nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("exchange and queue cannot be set together"))
	})

	It("Without exchange and queue should raise and error", func() {
		_, err := address(nil, nil, nil, nil)
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("exchange or queue must be set"))
	})

	It("With exchange and key should return address", func() {
		exchange := "my_exchange"
		key := "my_key"

		address, err := address(&exchange, &key, nil, nil)
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_exchange/my_key"))
	})

	It("With exchange should return address", func() {
		exchange := "my_exchange"
		address, err := address(&exchange, nil, nil, nil)
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_exchange"))
	})

	It("With exchange and key with names to encode should return the encoded address", func() {

		exchange := "my_ exchange/()"
		key := "my_key "

		address, err := address(&exchange, &key, nil, nil)
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_%20exchange%2F%28%29/my_key%20"))
	})

	It("With queue should return address", func() {
		queue := "my_queue>"
		address, err := address(nil, nil, &queue, nil)
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/queues/my_queue%3E"))
	})

	It("With queue and urlParameters should return address", func() {
		queue := "my_queue"
		address, err := purgeQueueAddress(&queue)
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/queues/my_queue/messages"))
	})

})
