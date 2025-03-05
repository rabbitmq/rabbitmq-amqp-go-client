package rabbitmqamqp

// test the OAuth2 connection

import (
	"context"
	"encoding/base64"
	"github.com/golang-jwt/jwt/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	testhelper "github.com/rabbitmq/rabbitmq-amqp-go-client/pkg/test-helper"
	"math/rand"
	"time"
)

const Base64Key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH"

// const  HmacKey KEY = new HmacKey(Base64.getDecoder().decode(Base64Key));
const AUDIENCE = "rabbitmq"

// Helper function to generate random string
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

var _ = Describe("OAuth2 Tests", func() {
	It("OAuth2 Connection should success", func() {
		tokenString := token(time.Now().Add(time.Duration(2500) * time.Millisecond))
		Expect(tokenString).NotTo(BeEmpty())

		conn, err := Dial(context.TODO(), "amqp://localhost:5672",
			&AmqpConnOptions{
				ContainerID: "oAuth2Test",
				OAuth2Options: &OAuth2Options{
					Token: tokenString,
				},
			})
		Expect(err).To(BeNil())
		Expect(conn).NotTo(BeNil())
		qName := generateName("OAuth2 Connection should success")
		_, err = conn.Management().DeclareQueue(context.Background(), &QuorumQueueSpecification{
			Name: qName,
		})
		Expect(err).To(BeNil())
		Expect(conn.Management().DeleteQueue(context.Background(), qName)).To(BeNil())
		Expect(conn.Close(context.Background())).To(BeNil())
	})

	It("OAuth2 Connection should disconnect after the timeout", func() {

		tokenString := token(time.Now().Add(time.Duration(1_000) * time.Millisecond))
		Expect(tokenString).NotTo(BeEmpty())

		conn, err := Dial(context.TODO(), "amqp://localhost:5672",
			&AmqpConnOptions{
				ContainerID: "oAuth2TestTimeout",
				OAuth2Options: &OAuth2Options{
					Token: tokenString,
				},
				RecoveryConfiguration: &RecoveryConfiguration{
					ActiveRecovery: false,
				},
			})
		Expect(err).To(BeNil())
		Expect(conn).NotTo(BeNil())
		ch := make(chan *StateChanged, 1)
		go func() {
			defer GinkgoRecover()
			for statusChanged := range ch {
				x := statusChanged.To.(*StateClosed)
				Expect(x.GetError()).NotTo(BeNil())
				Expect(x.GetError().Error()).To(ContainSubstring("credential expired"))
			}
		}()

		conn.NotifyStatusChange(ch)
		time.Sleep(1 * time.Second)
	})

	It("OAuth2 Connection should be alive after token refresh", func() {
		tokenString := token(time.Now().Add(time.Duration(1) * time.Second))
		Expect(tokenString).NotTo(BeEmpty())

		conn, err := Dial(context.TODO(), "amqp://localhost:5672",
			&AmqpConnOptions{
				ContainerID: "oAuth2Test",
				OAuth2Options: &OAuth2Options{
					Token: tokenString,
				},
				RecoveryConfiguration: &RecoveryConfiguration{
					ActiveRecovery: false,
				},
			})
		Expect(err).To(BeNil())
		Expect(conn).NotTo(BeNil())
		time.Sleep(100 * time.Millisecond)
		err = conn.RefreshToken(context.Background(), token(time.Now().Add(time.Duration(2500)*time.Millisecond)))
		time.Sleep(1 * time.Second)
		Expect(err).To(BeNil())
		Expect(conn.Close(context.Background())).To(BeNil())
	})

	// this test is a bit flaky, it may fail if the connection is not closed in time
	// that should mark as flakes

	It("OAuth2 Connection should use the new token to reconnect", func() {
		name := "oAuth2TestReconnect_" + time.Now().String()
		startToken := token(time.Now().Add(time.Duration(1) * time.Second))
		connection, err := Dial(context.Background(), "amqp://", &AmqpConnOptions{
			OAuth2Options: &OAuth2Options{
				Token: startToken,
			},
			ContainerID: name,
			// reduced the reconnect interval to speed up the test
			RecoveryConfiguration: &RecoveryConfiguration{
				ActiveRecovery:           true,
				BackOffReconnectInterval: 1100 * time.Millisecond,
				MaxReconnectAttempts:     5,
			},
		})

		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		ch := make(chan *StateChanged, 1)
		connection.NotifyStatusChange(ch)
		newToken := token(time.Now().Add(time.Duration(10) * time.Second))
		Expect(connection.RefreshToken(context.Background(), newToken)).To(BeNil())
		time.Sleep(1 * time.Second)
		// here the token used during the connection (startToken) is expired
		// the new token should be used to reconnect.
		// The test is to validate that the client uses the new token to reconnect
		// The RefreshToken requests a new token and updates the connection with the new token
		Eventually(func() bool {
			err := testhelper.DropConnectionContainerID(name)
			return err == nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		st1 := <-ch
		Expect(st1.From).To(Equal(&StateOpen{}))
		Expect(st1.To).To(BeAssignableToTypeOf(&StateClosed{}))

		time.Sleep(1 * time.Second)

		// the connection should not be reconnected
		Eventually(func() bool {
			conn, err := testhelper.GetConnectionByContainerID(name)
			return err == nil && conn != nil
		}).WithTimeout(5 * time.Second).WithPolling(400 * time.Millisecond).Should(BeTrue())
		Expect(connection.Close(context.Background())).To(BeNil())
	})

	It("Setting OAuth2 on the Environment should work", func() {
		env := NewClusterEnvironment([]Endpoint{
			{Address: "amqp://", Options: &AmqpConnOptions{
				OAuth2Options: &OAuth2Options{
					Token: token(time.Now().Add(time.Duration(10) * time.Second)),
				},
			},
			}})

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

	It("Can't use refresh token if not OAuth2 is enabled ", func() {
		connection, err := Dial(context.Background(), "amqp://", nil)
		Expect(err).To(BeNil())
		Expect(connection).NotTo(BeNil())
		err = connection.RefreshToken(context.Background(), token(time.Now().Add(time.Duration(10)*time.Second)))
		Expect(err).NotTo(BeNil())
		Expect(err.Error()).To(ContainSubstring("is not configured to use OAuth2 token"))
		Expect(connection.Close(context.Background())).To(BeNil())
	})

})

func token(duration time.Time) string {
	decodedKey, _ := base64.StdEncoding.DecodeString(Base64Key)

	claims := jwt.MapClaims{
		"iss":    "unit_test",
		"aud":    AUDIENCE,
		"exp":    jwt.NewNumericDate(duration),
		"scope":  []string{"rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"},
		"random": randomString(6),
	}

	// Create a new token object, specifying signing method and the claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = "token-key"

	// Sign and get the complete encoded token as a string using the secret
	tokenString, err := token.SignedString(decodedKey)
	Expect(err).To(BeNil())
	return tokenString
}
