package rabbitmqamqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Encode and decode address", func() {
	It("To address from address", func() {
		type ExpectedQueuesAndDestination struct {
			Queue   string
			Address string
		}
		e := []ExpectedQueuesAndDestination{
			{"queue with spaces", "/queues/queue%20with%20spaces"},
			{"queue+with+plus", "/queues/queue%2Bwith%2Bplus"},
			{"特殊字符", "/queues/%E7%89%B9%E6%AE%8A%E5%AD%97%E7%AC%A6"},
			{"myQueue", "/queues/myQueue"},
			{"queue/with/slash", "/queues/queue%2Fwith%2Fslash"},
			{"queue?with?question", "/queues/queue%3Fwith%3Fquestion"},
			{"emoji😊queue", "/queues/emoji%F0%9F%98%8Aqueue"},
			{"!@#$%^&*()", "/queues/%21%40%23%24%25%5E%26%2A%28%29"},
		}

		for i := range e {
			destination := &QueueAddress{
				Queue: e[i].Queue,
			}
			addr, err := destination.toAddress()
			Expect(err).To(BeNil())
			Expect(addr).To(Equal(e[i].Address))

			decodedQueue, err := parseQueueAddress(addr)
			Expect(err).To(BeNil())
			Expect(decodedQueue).To(Equal(e[i].Queue))
		}

	})
})
