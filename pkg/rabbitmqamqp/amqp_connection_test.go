package rabbitmqamqp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/Azure/go-amqp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
	"os"
	"sync"
	"time"
)

var _ = Describe("AMQP connection Test", func() {
	It("AMQP SASLTypeAnonymous connection should succeed", func() {
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous()})
		Expect(err).To(BeNil())
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})
	//
	It("AMQP SASLTypePlain connection should succeed", func() {

		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypePlain("guest", "guest")})

		Expect(err).To(BeNil())
		Expect(connection.Properties()["product"]).To(Equal("RabbitMQ"))

		err = connection.Close(context.Background())
		Expect(err).To(BeNil())
	})
	//
	It("AMQP connection should fail due to context cancellation", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		cancel()
		_, err := Dial(ctx, "amqp://", nil)
		Expect(err).NotTo(BeNil())
	})
	//
	It("AMQP connection should receive events", func() {
		ch := make(chan *StateChanged, 1)
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		connection.NotifyStatusChange(ch)
		err = connection.Close(context.Background())
		Expect(err).To(BeNil())

		recv := <-ch
		Expect(recv).NotTo(BeNil())
		Expect(recv.From).To(Equal(&StateOpen{}))
		Expect(recv.To).To(Equal(&StateClosed{}))
	})

	It("Entity tracker should be aligned with consumers and publishers  ", func() {
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			SASLType: amqp.SASLTypeAnonymous()})
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())

		queueName := generateNameWithDateTime("Entity tracker should be aligned with consumers and publishers")

		_, err = connection.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: queueName,
		})

		Expect(err).To(BeNil())
		publisher, err := connection.NewPublisher(context.Background(), &QueueAddress{Queue: queueName},
			&PublisherOptions{
				Id:             "my_id",
				SenderLinkName: "my_sender_link",
			})
		Expect(err).To(BeNil())
		Expect(publisher).NotTo(BeNil())
		consumer, err := connection.NewConsumer(context.Background(), queueName, nil)
		Expect(err).To(BeNil())
		Expect(consumer).NotTo(BeNil())
		// check the entity tracker
		Expect(connection.entitiesTracker).NotTo(BeNil())
		entLen := 0
		connection.entitiesTracker.consumers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(1))

		entLen = 0
		connection.entitiesTracker.publishers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(1))
		Expect(consumer.Close(context.Background())).To(BeNil())
		Expect(publisher.Close(context.Background())).To(BeNil())

		entLen = 0
		connection.entitiesTracker.consumers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(0))

		entLen = 0
		connection.entitiesTracker.publishers.Range(func(key, value interface{}) bool {
			entLen++
			return true
		})
		Expect(entLen).To(Equal(0))

		err = connection.Management().DeleteQueue(context.Background(), queueName)
		Expect(err).To(BeNil())

		Expect(connection.Close(context.Background())).To(BeNil())

	})

	Describe("AMQP TLS connection should succeed with SASLTypeAnonymous", func() {
		vhostTls := fmt.Sprintf("tls_%d", random(10_000_000))
		Expect(testhelper.CreateVirtualHost(vhostTls)).To(BeNil())
		wg := &sync.WaitGroup{}
		wg.Add(2)
		DescribeTable("TLS connection should succeed with SASLTypeAnonymous", func(virtualHost string) {
			// Load CA cert
			caCert, err := os.ReadFile("../../.ci/certs/ca_certificate.pem")
			Expect(err).To(BeNil())

			// Create a CA certificate pool and add the CA certificate to it
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)

			// Load client cert
			clientCert, err := tls.LoadX509KeyPair("../../.ci/certs/client_localhost_certificate.pem",
				"../../.ci/certs/client_localhost_key.pem")
			Expect(err).To(BeNil())

			// Create a TLS configuration
			tlsConfig := &tls.Config{
				Certificates:       []tls.Certificate{clientCert},
				RootCAs:            caCertPool,
				InsecureSkipVerify: false,
				ServerName:         "localhost",
			}

			// Dial the AMQP server with TLS configuration
			connection, err := Dial(context.Background(), fmt.Sprintf("amqps://localhost:5671/%s", virtualHost), &AmqpConnOptions{
				SASLType:  amqp.SASLTypeAnonymous(),
				TLSConfig: tlsConfig,
			})
			Expect(err).To(BeNil())
			Expect(connection).NotTo(BeNil())

			// Close the connection
			err = connection.Close(context.Background())
			Expect(err).To(BeNil())
			wg.Done()
		},
			Entry("with virtual host", "/"),
			Entry("with a not default virtual host", vhostTls),
		)
		go func() {
			wg.Wait()
			Expect(testhelper.DeleteVirtualHost(vhostTls)).To(BeNil())
		}()

	})

})
