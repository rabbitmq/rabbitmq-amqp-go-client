package main

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"time"
)

func main() {

	rmq.Info("Golang AMQP 1.0 Advanced connection settings")

	// rmq.NewClusterEnvironment setups the environment.
	// define multiple endpoints with different connection settings
	// the connection will be created based on the strategy Sequential
	env := rmq.NewClusterEnvironment([]rmq.Endpoint{

		//this is correct
		{Address: "amqp://localhost:5672", Options: &rmq.AmqpConnOptions{
			ContainerID: "My connection one ",
			SASLType:    amqp.SASLTypeAnonymous(),
			RecoveryConfiguration: &rmq.RecoveryConfiguration{
				ActiveRecovery: false,
			},
			OAuth2Options: nil,
			Id:            "my first id",
		}},
		// this is correct
		{Address: "amqp://localhost:5672", Options: &rmq.AmqpConnOptions{
			ContainerID: "My connection two",
			SASLType:    amqp.SASLTypePlain("guest", "guest"),
			RecoveryConfiguration: &rmq.RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 2 * time.Second,
				MaxReconnectAttempts:     5,
			},
			OAuth2Options: nil,
			Id:            "my second id",
		}},

		//this end point is incorrect, so won't be used
		//so another endpoint will be used
		{Address: "amqp://wrong:5672", Options: &rmq.AmqpConnOptions{
			ContainerID: "My connection wrong",
			SASLType:    amqp.SASLTypePlain("guest", "guest"),
			RecoveryConfiguration: &rmq.RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 2 * time.Second,
				MaxReconnectAttempts:     5,
			},
			OAuth2Options: nil,
			Id:            "my wrong id",
		}},
	}, rmq.WithStrategy(rmq.StrategyRandom))

	for i := 0; i < 5; i++ {
		connection, err := env.NewConnection(context.Background())
		if err != nil {
			rmq.Error("Error opening connection", err)
			return
		}

		rmq.Info("Connection opened", "Container ID", connection.Id())
		time.Sleep(200 * time.Millisecond)

	}
	// Here you should see the connection opened for the first two endpoints
	// with the containers ID "My connection one" and with the containers ID "My connection two"
	// press any key to exit
	fmt.Println("Press any key to exit")
	var input string
	_, _ = fmt.Scanln(&input)

}
