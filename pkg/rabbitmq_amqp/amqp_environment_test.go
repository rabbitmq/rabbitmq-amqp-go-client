package rabbitmq_amqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Environment Test", func() {
	It("AMQP Environment Connection should succeed", func() {
		env := NewEnvironment([]string{"amqp://"}, nil)
		Expect(env).NotTo(BeNil())
		Expect(env.Connections()).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(0))

		connection, err := env.NewConnection(context.Background())
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(1))
		Expect(connection.Close(context.Background())).To(BeNil())
		Expect(len(env.Connections())).To(Equal(0))
	})

	It("AMQP Environment CloseConnections should remove all the elements form the list", func() {
		env := NewEnvironment([]string{"amqp://"}, nil)
		Expect(env).NotTo(BeNil())
		Expect(env.Connections()).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(0))

		connection, err := env.NewConnection(context.Background())
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(1))

		Expect(env.CloseConnections(context.Background())).To(BeNil())
		Expect(len(env.Connections())).To(Equal(0))
	})

	It("AMQP Environment Connection ID should be unique", func() {
		env := NewEnvironment([]string{"amqp://"}, nil)
		Expect(env).NotTo(BeNil())
		Expect(env.Connections()).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(0))
		connection, err := env.NewConnection(context.Background(), "myConnectionId")
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(1))
		connectionShouldBeNil, err := env.NewConnection(context.Background(), "myConnectionId")
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("connection with id myConnectionId already exists"))
		Expect(connectionShouldBeNil).To(BeNil())
		Expect(len(env.Connections())).To(Equal(1))
		Expect(connection.Close(context.Background())).To(BeNil())
		Expect(len(env.Connections())).To(Equal(0))

	})
})
