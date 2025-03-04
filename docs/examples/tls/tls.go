package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"github.com/Azure/go-amqp"
	rmq "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/rabbitmqamqp"
	"os"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	// to run the example you can use the certificates from the rabbitmq-amqp-go-client
	// inside the directory .ci/certs
	caCert, err := os.ReadFile("/path/ca_certificate.pem")
	check(err)
	// Create a CA certificate pool and add the CA certificate to it
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Load client cert
	clientCert, err := tls.LoadX509KeyPair("/path//client_localhost_certificate.pem",
		"/path//client_localhost_key.pem")
	check(err)

	// Create a TLS configuration
	tlsConfig := &tls.Config{
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
		ServerName:         "localhost", // the server name should match the name on the certificate
	}

	env := rmq.NewClusterEnvironment([]rmq.Endpoint{
		{Address: "amqps://localhost:5671", Options: &rmq.AmqpConnOptions{
			SASLType:  amqp.SASLTypeAnonymous(),
			TLSConfig: tlsConfig,
		}},
	})

	connection, err := env.NewConnection(context.Background())
	check(err)

	// Close the connection
	err = connection.Close(context.Background())
	check(err)

}
