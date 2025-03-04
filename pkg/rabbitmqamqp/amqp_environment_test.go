package rabbitmqamqp

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP Environment Test", func() {
	It("AMQP Environment connection should succeed", func() {
		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://"}})
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
		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://"}})
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

	It("Get new connection should connect to the one correct uri and fails the others", func() {

		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://localhost:1234"}, {Address: "amqp://nohost:555"}, {Address: "amqp://"}})
		conn, err := env.NewConnection(context.Background())
		Expect(err).To(BeNil())
		Expect(conn.Close(context.Background()))
	})

	It("Get new connection should fail due of wrong Port", func() {
		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://localhost:1234"}})
		_, err := env.NewConnection(context.Background())
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should fail due of wrong Host", func() {
		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://wrong_host:5672"}})
		_, err := env.NewConnection(context.Background())
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should fails with all the wrong uris", func() {
		env := NewClusterEnvironment([]Endpoint{{Address: "amqp://localhost:1234"}, {Address: "amqp://nohost:555"}, {Address: "amqp://nono"}})
		_, err := env.NewConnection(context.Background())
		Expect(err).NotTo(BeNil())
	})

	It("AMQP connection should success in different vhosts", func() {
		// user_1 and vhost_user_1 are preloaded in the rabbitmq server during the startup
		env := NewEnvironment("amqp://user_1:user_1@localhost:5672/vhost_user_1", nil)
		Expect(env).NotTo(BeNil())
		Expect(env.Connections()).NotTo(BeNil())
		Expect(len(env.Connections())).To(Equal(0))
		conn, err := env.NewConnection(context.Background())
		Expect(err).To(BeNil())
		Expect(conn.Close(context.Background()))
	})

	It("AMQP connection should fail with user_1 does not have the grant for /", func() {
		// user_1 is preloaded in the rabbitmq server during the startup
		env := NewEnvironment("amqp://user_1:user_1@localhost:5672/", nil)
		Expect(env).NotTo(BeNil())
		_, err := env.NewConnection(context.Background())
		Expect(err).NotTo(BeNil())
	})

	Describe("Environment strategy", func() {
		DescribeTable("Environment with strategy should success", func(strategy TEndPointStrategy) {
			env := NewClusterEnvironmentWithStrategy([]Endpoint{{Address: "amqp://", Options: &AmqpConnOptions{Id: "my"}}, {Address: "amqp://nohost:555"}, {Address: "amqp://nono"}}, StrategyRandom)
			//env := NewClusterEnvironmentWithStrategy([]Endpoint{{Address: "amqp://", Options: &AmqpConnOptions{Id: "my"}}}, strategy)
			Expect(env).NotTo(BeNil())
			Expect(env.Connections()).NotTo(BeNil())
			Expect(len(env.Connections())).To(Equal(0))
			conn, err := env.NewConnection(context.Background())
			Expect(err).To(BeNil())
			Expect(conn.Id()).To(Equal("my_1"))
			Expect(conn.Close(context.Background()))
		},
			Entry("StrategyRandom", StrategyRandom),
			Entry("StrategySequential", StrategySequential),
		)
	})

})
