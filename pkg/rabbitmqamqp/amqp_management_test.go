package rabbitmqamqp

import (
	"context"
	"time"

	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Management tests", func() {
	It("AMQP Management should fail due to context cancellation", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		err = connection.Management().Open(ctx, connection)
		Expect(err).NotTo(BeNil())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("AMQP Management should receive events", func() {
		ch := make(chan *StateChanged, 2)
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous(),
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery: false,
			},
		})
		Expect(err).To(BeNil())
		connection.NotifyStatusChange(ch)
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
		recv := <-ch
		Expect(recv).NotTo(BeNil())

		Expect(recv.From).To(Equal(&StateOpen{}))
		Expect(recv.To).To(Equal(&StateClosed{}))
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Request", func() {

		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())

		management := connection.Management()
		kv := make(map[string]any)
		kv["durable"] = true
		kv["auto_delete"] = true
		_queueArguments := make(map[string]any)
		_queueArguments["x-queue-type"] = "classic"
		kv["arguments"] = _queueArguments
		path := "/queues/test"
		result, err := management.Request(context.Background(), kv, path, "PUT", []int{200})
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		result, err = management.Request(context.Background(), amqp.Null{}, path, "DELETE", []int{responseCode200})
		Expect(err).To(BeNil())
		Expect(result).NotTo(BeNil())
		Expect(management.Close(context.Background())).To(BeNil())

		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("GET on non-existing queue returns ErrDoesNotExist", func() {

		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())

		management := connection.Management()
		path := "/queues/i-do-not-exist"
		result, err := management.Request(context.Background(), amqp.Null{}, path, commandGet, []int{responseCode200, responseCode404})
		Expect(err).To(Equal(ErrDoesNotExist))
		Expect(result).To(BeNil())
	})

	// Unit tests for isQueueDestinationForBindingTransient function
	Context("isQueueDestinationForBindingTransient", func() {
		var management *AmqpManagement

		BeforeEach(func() {
			management = newAmqpManagement(TopologyRecoveryAllEnabled)
			management.topologyRecoveryRecords = newTopologyRecoveryRecords()
		})

		Context("with ExchangeToQueueBindingSpecification", func() {
			It("returns false when binding destination is not a queue", func() {
				// This won't happen with ExchangeToQueueBindingSpecification
				// since it always returns true for isDestinationQueue()
				// but we test the logic anyway
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "test-queue",
					BindingKey:       "test-key",
				}

				// Empty queue records
				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeFalse())
			})

			It("returns true when queue is transient with autoDelete=true",
				func() {
					binding := &ExchangeToQueueBindingSpecification{
						SourceExchange:   "test-exchange",
						DestinationQueue: "transient-queue",
						BindingKey:       "test-key",
					}

					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "transient-queue",
							queueType:  Classic,
							autoDelete: ptr(true),
							exclusive:  ptr(false),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeTrue())
				})

			It("returns true when queue is transient with exclusive=true",
				func() {
					binding := &ExchangeToQueueBindingSpecification{
						SourceExchange:   "test-exchange",
						DestinationQueue: "exclusive-queue",
						BindingKey:       "test-key",
					}

					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "exclusive-queue",
							queueType:  Classic,
							autoDelete: ptr(false),
							exclusive:  ptr(true),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeTrue())
				})

			It("returns true when queue has both autoDelete and exclusive true",
				func() {
					binding := &ExchangeToQueueBindingSpecification{
						SourceExchange:   "test-exchange",
						DestinationQueue: "both-transient-queue",
						BindingKey:       "test-key",
					}

					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "both-transient-queue",
							queueType:  Classic,
							autoDelete: ptr(true),
							exclusive:  ptr(true),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeTrue())
				})

			It("returns false when queue is not transient", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "durable-queue",
					BindingKey:       "test-key",
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "durable-queue",
						queueType:  Classic,
						autoDelete: ptr(false),
						exclusive:  ptr(false),
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeFalse())
			})

			It("returns false when queue does not exist in records", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "non-existent-queue",
					BindingKey:       "test-key",
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "different-queue",
						queueType:  Classic,
						autoDelete: ptr(true),
						exclusive:  ptr(false),
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeFalse())
			})

			It("returns false when queue has nil autoDelete and exclusive", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "quorum-queue",
					BindingKey:       "test-key",
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "quorum-queue",
						queueType:  Quorum,
						autoDelete: nil,
						exclusive:  nil,
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeFalse())
			})

			It("returns correct result with multiple queues in records", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "target-queue",
					BindingKey:       "test-key",
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "queue-1",
						queueType:  Classic,
						autoDelete: ptr(false),
						exclusive:  ptr(false),
					},
					{
						queueName:  "target-queue",
						queueType:  Classic,
						autoDelete: ptr(true),
						exclusive:  ptr(false),
					},
					{
						queueName:  "queue-3",
						queueType:  Classic,
						autoDelete: ptr(false),
						exclusive:  ptr(true),
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeTrue())
			})

			It("returns true when queue has autoDelete=nil and exclusive=true",
				func() {
					binding := &ExchangeToQueueBindingSpecification{
						SourceExchange:   "test-exchange",
						DestinationQueue: "mixed-queue",
						BindingKey:       "test-key",
					}

					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "mixed-queue",
							queueType:  Stream,
							autoDelete: nil,
							exclusive:  ptr(true),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeTrue())
				})
		})

		Context("with ExchangeToExchangeBindingSpecification", func() {
			It("returns false because destination is an exchange, not a queue",
				func() {
					binding := &ExchangeToExchangeBindingSpecification{
						SourceExchange:      "source-exchange",
						DestinationExchange: "dest-exchange",
						BindingKey:          "test-key",
					}

					// Even if we have a queue with the same name as destination
					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "dest-exchange",
							queueType:  Classic,
							autoDelete: ptr(true),
							exclusive:  ptr(true),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeFalse())
				})

			It("returns false with empty queue records", func() {
				binding := &ExchangeToExchangeBindingSpecification{
					SourceExchange:      "source-exchange",
					DestinationExchange: "dest-exchange",
					BindingKey:          "test-key",
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeFalse())
			})

			It("returns false even with multiple transient queues in records",
				func() {
					binding := &ExchangeToExchangeBindingSpecification{
						SourceExchange:      "source-exchange",
						DestinationExchange: "dest-exchange",
						BindingKey:          "test-key",
					}

					management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
						{
							queueName:  "queue-1",
							queueType:  Classic,
							autoDelete: ptr(true),
							exclusive:  ptr(false),
						},
						{
							queueName:  "queue-2",
							queueType:  Classic,
							autoDelete: ptr(false),
							exclusive:  ptr(true),
						},
					}

					result := management.isQueueDestinationForBindingTransient(binding)
					Expect(result).To(BeFalse())
				})
		})

		Context("edge cases with binding arguments", func() {
			It("works correctly with bindings that have arguments", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "transient-queue",
					BindingKey:       "test-key",
					Arguments: map[string]any{
						"x-match": "all",
						"key1":    "value1",
					},
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "transient-queue",
						queueType:  Classic,
						autoDelete: ptr(true),
						exclusive:  ptr(false),
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeTrue())
			})

			It("works correctly with empty binding key", func() {
				binding := &ExchangeToQueueBindingSpecification{
					SourceExchange:   "test-exchange",
					DestinationQueue: "transient-queue",
					BindingKey:       "",
				}

				management.topologyRecoveryRecords.queues = []queueRecoveryRecord{
					{
						queueName:  "transient-queue",
						queueType:  Classic,
						autoDelete: ptr(false),
						exclusive:  ptr(true),
					},
				}

				result := management.isQueueDestinationForBindingTransient(binding)
				Expect(result).To(BeTrue())
			})
		})
	})
})
