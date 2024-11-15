package rabbitmq_amqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address builder test ", func() {
	It("With exchange, queue and key should raise and error", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Queue("queue").Exchange("exchange").Key("key")
		_, err := addressBuilder.Address()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("exchange and queue cannot be set together"))
	})

	It("Without exchange and queue should raise and error", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		_, err := addressBuilder.Address()
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(Equal("exchange or queue must be set"))
	})

	It("With exchange and key should return address", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Exchange("my_exchange").Key("my_key")
		address, err := addressBuilder.Address()
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_exchange/my_key"))
	})

	It("With exchange should return address", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Exchange("my_exchange")
		address, err := addressBuilder.Address()
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_exchange"))
	})

	It("With exchange and key with names to encode should return the encoded address", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Exchange("my_ exchange/()").Key("my_key ")
		address, err := addressBuilder.Address()
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/exchanges/my_%20exchange%2F%28%29/my_key%20"))
	})

	It("With queue should return address", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Queue("my_queue>")
		address, err := addressBuilder.Address()
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/queues/my_queue%3E"))
	})

	It("With queue and append should return address", func() {
		addressBuilder := NewAddressBuilder()
		Expect(addressBuilder).NotTo(BeNil())
		Expect(addressBuilder).To(BeAssignableToTypeOf(&AddressBuilder{}))
		addressBuilder.Queue("my_queue").Append("/messages")
		address, err := addressBuilder.Address()
		Expect(err).To(BeNil())
		Expect(address).To(Equal("/queues/my_queue/messages"))
	})

})
