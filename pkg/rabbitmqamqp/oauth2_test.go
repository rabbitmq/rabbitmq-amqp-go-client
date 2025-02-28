package rabbitmqamqp

// test the OAuth2 connection

import (
	"context"
	"encoding/base64"
	"github.com/Azure/go-amqp"
	"github.com/golang-jwt/jwt/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		decodedKey, _ := base64.StdEncoding.DecodeString(Base64Key)

		claims := jwt.MapClaims{
			"iss":    "unit_test",
			"aud":    AUDIENCE,
			"exp":    jwt.NewNumericDate(time.Now().Add(time.Duration(2500) * time.Millisecond)),
			"scope":  []string{"rabbitmq.configure:*/*", "rabbitmq.write:*/*", "rabbitmq.read:*/*"},
			"random": randomString(6),
		}

		// Create a new token object, specifying signing method and the claims
		token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
		token.Header["kid"] = "token-key"

		// Sign and get the complete encoded token as a string using the secret
		tokenString, err := token.SignedString(decodedKey)
		Expect(err).To(BeNil())
		Expect(tokenString).NotTo(BeEmpty())

		conn, err := Dial(context.TODO(), []string{"amqp://"},
			&AmqpConnOptions{
				ContainerID: "oAuth2Test",
				SASLType:    amqp.SASLTypePlain("", tokenString),
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
})
