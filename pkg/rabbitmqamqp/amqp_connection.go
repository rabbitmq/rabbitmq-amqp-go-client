package rabbitmqamqp

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var ErrConnectionClosed = errors.New("connection is closed")

type AmqpAddress struct {
	// the address of the AMQP server
	// it is in the form of amqp://<host>:<port>
	// or amqps://<host>:<port>
	// the port is optional
	// the default port is 5672
	// the default protocol is amqp
	// the default host is localhost
	// the default virtual host is "/"
	// the default user is guest
	// the default password is guest
	// the default SASL type is SASLTypeAnonymous
	Address string
	// Options: Additional options for the connection
	Options *AmqpConnOptions
}

type OAuth2Options struct {
	Token string
}

func (o OAuth2Options) Clone() *OAuth2Options {
	cloned := &OAuth2Options{
		Token: o.Token,
	}
	return cloned

}

// TopologyRecoveryOptions is used to configure the topology recovery behavior of the connection.
// See [TopologyRecoveryDisabled], [TopologyRecoveryOnlyTransient], and [TopologyRecoveryAllEnabled] for more information.
type TopologyRecoveryOptions byte

const (
	// TopologyRecoveryOnlyTransient recovers only queues declared as exclusive and/or auto delete, and
	// related bindings. Exchanges are not recovered.
	TopologyRecoveryOnlyTransient TopologyRecoveryOptions = iota
	// TopologyRecoveryDisabled disables the topology recovery.
	TopologyRecoveryDisabled
	// TopologyRecoveryAllEnabled recovers all the topology. All exchanges, queues, and bindings are recovered.
	TopologyRecoveryAllEnabled
)

type AmqpConnOptions struct {
	// wrapper for amqp.ConnOptions
	ContainerID string

	// wrapper for amqp.ConnOptions
	HostName string
	// wrapper for amqp.ConnOptions
	IdleTimeout time.Duration

	// wrapper for amqp.ConnOptions
	MaxFrameSize uint32

	// wrapper for amqp.ConnOptions
	MaxSessions uint16

	// wrapper for amqp.ConnOptions
	Properties map[string]any

	// wrapper for amqp.ConnOptions
	SASLType amqp.SASLType

	// wrapper for amqp.ConnOptions
	TLSConfig *tls.Config

	// wrapper for amqp.ConnOptions
	WriteTimeout time.Duration

	// RecoveryConfiguration is used to configure the recovery behavior of the connection.
	// when the connection is closed unexpectedly.
	RecoveryConfiguration *RecoveryConfiguration

	// TopologyRecoveryOptions is used to configure the topology recovery behavior of the connection.
	TopologyRecoveryOptions TopologyRecoveryOptions

	// The OAuth2Options is used to configure the connection with OAuth2 token.
	OAuth2Options *OAuth2Options

	// Local connection identifier (not sent to the server)
	// if not provided, a random UUID is generated
	Id string
}

func (a *AmqpConnOptions) isOAuth2() bool {
	return a.OAuth2Options != nil
}

func (a *AmqpConnOptions) Clone() *AmqpConnOptions {

	cloned := &AmqpConnOptions{
		ContainerID:             a.ContainerID,
		IdleTimeout:             a.IdleTimeout,
		MaxFrameSize:            a.MaxFrameSize,
		MaxSessions:             a.MaxSessions,
		Properties:              a.Properties,
		SASLType:                a.SASLType,
		TLSConfig:               a.TLSConfig,
		WriteTimeout:            a.WriteTimeout,
		Id:                      a.Id,
		TopologyRecoveryOptions: a.TopologyRecoveryOptions,
	}
	if a.OAuth2Options != nil {
		cloned.OAuth2Options = a.OAuth2Options.Clone()
	}
	if a.RecoveryConfiguration != nil {
		cloned.RecoveryConfiguration = a.RecoveryConfiguration.Clone()
	}

	return cloned
}

type AmqpConnection struct {
	properties        map[string]any
	featuresAvailable *featuresAvailable

	azureConnection         *amqp.Conn
	management              *AmqpManagement
	lifeCycle               *LifeCycle
	amqpConnOptions         *AmqpConnOptions
	address                 string
	session                 *amqp.Session
	refMap                  *sync.Map
	entitiesTracker         *entitiesTracker
	topologyRecoveryRecords *topologyRecoveryRecords
	metricsCollector        MetricsCollector
	mutex                   sync.RWMutex
	closed                  bool

	// Server connection info for OTEL semantic conventions
	serverAddress string // Parsed from URI (server.address)
	serverPort    int    // Parsed from URI (server.port)
}

func (a *AmqpConnection) Properties() map[string]any {
	return a.properties

}

// NewPublisher creates a new Publisher that sends messages to the provided destination.
// The destination is a ITargetAddress that can be a Queue or an Exchange with a routing key.
// options is an IPublisherOptions that can be used to configure the publisher.
// See QueueAddress and ExchangeAddress for more information.
func (a *AmqpConnection) NewPublisher(ctx context.Context, destination ITargetAddress, options IPublisherOptions) (*Publisher, error) {
	destinationAdd := ""
	err := error(nil)
	if destination != nil {
		destinationAdd, err = destination.toAddress()
		if err != nil {
			return nil, err
		}
		err = validateAddress(destinationAdd)
		if err != nil {
			return nil, err
		}
	}

	return newPublisher(ctx, a, destinationAdd, options)
}

// NewConsumer creates a new Consumer that listens to the provided Queue
// options is an IConsumerOptions that can be used to configure the consumer.
// it can be nil, and the consumer will be created with default options.
// see
func (a *AmqpConnection) NewConsumer(ctx context.Context, queueName string, options IConsumerOptions) (*Consumer, error) {

	if options != nil {
		err := options.validate(a.featuresAvailable)
		if err != nil {
			return nil, err
		}
	}

	if options != nil && options.isDirectReplyToEnable() {
		return newConsumer(ctx, a, "", options)
	}

	destination := &QueueAddress{
		Queue: queueName,
	}
	destinationAdd, err := destination.toAddress()
	if err != nil {
		return nil, err
	}
	return newConsumer(ctx, a, destinationAdd, options)
}

// NewResponder creates a new RPC server that processes requests from the
// specified queue. The requestQueue in options is mandatory, while other
// fields are optional and will use defaults if not provided.
func (a *AmqpConnection) NewResponder(ctx context.Context, options ResponderOptions) (Responder, error) {
	if err := options.validate(); err != nil {
		return nil, fmt.Errorf("rpc server options validation: %w", err)
	}

	// Create consumer for receiving requests
	// consumer, err := a.NewConsumer(ctx, options.RequestQueue, nil)
	consumer, err := a.NewConsumer(ctx, options.RequestQueue, &ConsumerOptions{InitialCredits: -1})
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	consumer.issueCredits(1)

	// Create publisher for sending replies
	publisher, err := a.NewPublisher(ctx, nil, nil)
	if err != nil {
		consumer.Close(ctx) // cleanup consumer on failure
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	// Set defaults for optional fields
	handler := options.Handler
	if handler == nil {
		handler = noOpHandler
	}

	correlationIdExtractor := options.CorrelationIdExtractor
	if correlationIdExtractor == nil {
		correlationIdExtractor = defaultCorrelationIdExtractor
	}

	replyPostProcessor := options.ReplyPostProcessor
	if replyPostProcessor == nil {
		replyPostProcessor = defaultReplyPostProcessor
	}

	server := &amqpResponder{
		requestHandler:         handler,
		requestQueue:           options.RequestQueue,
		publisher:              publisher,
		consumer:               consumer,
		correlationIdExtractor: correlationIdExtractor,
		replyPostProcessor:     replyPostProcessor,
	}
	go server.handle()

	return server, nil
}

// NewRequester creates a new RPC client that sends requests to the specified queue
// and receives replies on a dynamically created reply queue.
func (a *AmqpConnection) NewRequester(ctx context.Context, options *RequesterOptions) (Requester, error) {
	if options == nil {
		return nil, fmt.Errorf("options cannot be nil")
	}
	if options.RequestQueueName == "" {
		return nil, fmt.Errorf("request QueueName is mandatory")
	}

	// Create publisher for sending requests
	requestQueue := &QueueAddress{
		Queue: options.RequestQueueName,
	}
	publisher, err := a.NewPublisher(ctx, requestQueue, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create publisher: %w", err)
	}

	replyQueueName := options.ReplyToQueueName
	queueName := ""
	if !options.DirectReplyTo {

		if len(replyQueueName) == 0 {
			replyQueueName = generateNameWithDefaultPrefix()
		}

		// Declare reply queue as exclusive, auto-delete classic queue
		q, err := a.management.DeclareQueue(ctx, &ClassicQueueSpecification{
			Name:         replyQueueName,
			IsExclusive:  true,
			IsAutoDelete: true,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to declare reply queue: %w", err)
		}
		queueName = q.Name()
	}

	// Set defaults for optional fields
	correlationIdSupplier := options.CorrelationIdSupplier
	if correlationIdSupplier == nil {
		correlationIdSupplier = newRandomUuidCorrelationIdSupplier()
	}

	requestPostProcessor := options.RequestPostProcessor
	if requestPostProcessor == nil {
		requestPostProcessor = func(request *amqp.Message, correlationID any) *amqp.Message {
			if request.Properties == nil {
				request.Properties = &amqp.MessageProperties{}
			}
			request.Properties.MessageID = correlationID
			return request
		}
	}

	requestTimeout := options.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = DefaultRpcRequestTimeout
	}

	correlationIdExtractor := options.CorrelationIdExtractor
	if correlationIdExtractor == nil {
		correlationIdExtractor = defaultReplyCorrelationIdExtractor
	}

	client := &amqpRequester{
		requestQueue:           requestQueue,
		replyToQueue:           &QueueAddress{Queue: replyQueueName},
		publisher:              publisher,
		requestPostProcessor:   requestPostProcessor,
		correlationIdSupplier:  correlationIdSupplier,
		correlationIdExtractor: correlationIdExtractor,
		requestTimeout:         requestTimeout,
		pendingRequests:        make(map[any]*outstandingRequest),
		done:                   make(chan struct{}),
	}

	feature := DefaultSettle
	if options.DirectReplyTo {
		feature = DirectReplyTo
	}
	// Create consumer for receiving replies
	consumer, err := a.NewConsumer(ctx, queueName, &ConsumerOptions{
		Feature: feature,
	})
	if err != nil {
		_ = publisher.Close(ctx) // cleanup publisher on failure
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}

	client.consumer = consumer
	reply, err := consumer.GetQueue()
	if err != nil {
		_ = publisher.Close(ctx) // cleanup publisher on failure
		_ = consumer.Close(ctx)
		return nil, fmt.Errorf("failed to get reply queue: %w", err)
	}

	client.replyToQueue = &QueueAddress{Queue: reply}

	go client.messageReceivedHandler()
	go client.requestTimeoutTask()

	return client, nil
}

// Dial connect to the AMQP 1.0 server using the provided connectionSettings
// Returns a pointer to the new AmqpConnection if successful else an error.
//
// TODO: perhaps deprecate this function and use Environment.NewConnection function instead?
func Dial(ctx context.Context, address string, connOptions *AmqpConnOptions) (*AmqpConnection, error) {
	return dialWithMetrics(ctx, address, connOptions, nil)
}

// dialWithMetrics is the internal function that creates a connection with a metrics collector.
// If metricsCollector is nil, a no-op implementation is used.
func dialWithMetrics(ctx context.Context, address string, connOptions *AmqpConnOptions, metricsCollector MetricsCollector) (*AmqpConnection, error) {
	connOptions, err := validateOptions(connOptions)
	if err != nil {
		return nil, err
	}

	if metricsCollector == nil {
		metricsCollector = DefaultMetricsCollector()
	}

	// Parse URI to extract server address and port for OTEL semantic conventions
	uri, err := ParseURI(address)
	if err != nil {
		return nil, err
	}

	// create the connection
	conn := &AmqpConnection{
		management:              newAmqpManagement(connOptions.TopologyRecoveryOptions),
		lifeCycle:               NewLifeCycle(),
		amqpConnOptions:         connOptions,
		entitiesTracker:         newEntitiesTracker(),
		topologyRecoveryRecords: newTopologyRecoveryRecords(),
		featuresAvailable:       newFeaturesAvailable(),
		metricsCollector:        metricsCollector,
		serverAddress:           uri.Host,
		serverPort:              uri.Port,
	}

	// management needs to access the connection to manage the recovery records
	conn.management.topologyRecoveryRecords = conn.topologyRecoveryRecords

	err = conn.open(ctx, address, connOptions)
	if err != nil {
		return nil, err
	}
	conn.amqpConnOptions = connOptions
	conn.address = address
	conn.lifeCycle.SetState(&StateOpen{})

	// Record the connection opening metric
	conn.metricsCollector.OpenConnection()

	return conn, nil
}

func validateOptions(connOptions *AmqpConnOptions) (*AmqpConnOptions, error) {
	if connOptions == nil {
		connOptions = &AmqpConnOptions{}
	}
	if connOptions.SASLType == nil {
		// RabbitMQ requires SASL security layer
		// to be enabled for AMQP 1.0 connections.
		// So this is mandatory and default in case not defined.
		connOptions.SASLType = amqp.SASLTypeAnonymous()
	}

	if connOptions.Id == "" {
		connOptions.Id = uuid.New().String()
	}

	// In case of OAuth2 token, the SASLType should be set to SASLTypePlain
	if connOptions.isOAuth2() {
		if connOptions.OAuth2Options.Token == "" {
			return nil, fmt.Errorf("OAuth2 token is empty")
		}
		connOptions.SASLType = amqp.SASLTypePlain("", connOptions.OAuth2Options.Token)
	}

	if connOptions.RecoveryConfiguration == nil {
		connOptions.RecoveryConfiguration = NewRecoveryConfiguration()
	}

	// validate the RecoveryConfiguration options
	if connOptions.RecoveryConfiguration.MaxReconnectAttempts <= 0 && connOptions.RecoveryConfiguration.ActiveRecovery {
		return nil, fmt.Errorf("MaxReconnectAttempts should be greater than 0")
	}
	if connOptions.RecoveryConfiguration.BackOffReconnectInterval <= 1*time.Second && connOptions.RecoveryConfiguration.ActiveRecovery {
		return nil, fmt.Errorf("BackOffReconnectInterval should be greater than 1 second")
	}

	return connOptions, nil
}

// Open opens a connection to the AMQP 1.0 server.
// using the provided connectionSettings and the AMQPLite library.
// Setups the connection and the management interface.
func (a *AmqpConnection) open(ctx context.Context, address string, connOptions *AmqpConnOptions) error {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	if a.closed {
		return ErrConnectionClosed
	}

	// random pick and extract one address to use for connection
	var azureConnection *amqp.Conn
	//connOptions.hostName is the  way to set the virtual host
	// so we need to pre-parse the URI to get the virtual host
	// the PARSE is copied from go-amqp091 library
	// the URI will be parsed is parsed again in the amqp lite library
	uri, err := ParseURI(address)
	if err != nil {
		return err
	}

	amqpLiteConnOptions := &amqp.ConnOptions{
		ContainerID:  connOptions.ContainerID,
		HostName:     fmt.Sprintf("vhost:%s", uri.Vhost),
		IdleTimeout:  connOptions.IdleTimeout,
		MaxFrameSize: connOptions.MaxFrameSize,
		MaxSessions:  connOptions.MaxSessions,
		Properties:   connOptions.Properties,
		SASLType:     connOptions.SASLType,
		TLSConfig:    connOptions.TLSConfig,
		WriteTimeout: connOptions.WriteTimeout,
	}

	u, err := url.Parse(address)
	if err != nil {
		return err
	}

	if u.Scheme == "ws" || u.Scheme == "wss" {

		wsAddress, wsHeaders, err := sanitizeWebSocketURL(address)
		if err != nil {
			Error("Failed to sanitize websocket URL", "url", ExtractWithoutPassword(address), "error", err, "ID", connOptions.Id)
			return fmt.Errorf("failed to sanitize websocket URL: %w", err)
		}

		// Create a WebSocket dialer
		dialer := websocket.DefaultDialer
		if u.Scheme == "wss" && connOptions.TLSConfig != nil {
			dialer.TLSClientConfig = connOptions.TLSConfig
		}

		// Dial the WebSocket server
		wsConn, _, err := dialer.Dial(wsAddress, wsHeaders)
		if err != nil {
			Error("Failed to open a websocket connection", "url", ExtractWithoutPassword(wsAddress), "error", err, "ID", connOptions.Id)
			return fmt.Errorf("failed to open a websocket connection: %w", err)
		}

		// Wrap the WebSocket connection in a WebSocketConn
		neConn := NewWebSocketConn(wsConn)
		azureConnection, err = amqp.NewConn(ctx, neConn, amqpLiteConnOptions)
		if err != nil {
			Error("Failed to open AMQP over WebSocket connection", "url", ExtractWithoutPassword(address), "error", err, "ID", connOptions.Id)
		}
	} else {
		azureConnection, err = amqp.Dial(ctx, address, amqpLiteConnOptions)
		if err != nil && (connOptions.TLSConfig != nil || uri.Scheme == AMQPS) {
			Error("Failed to open TLS connection", fmt.Sprintf("%s://%s", uri.Scheme, uri.Host), err, "ID", connOptions.Id)
			return fmt.Errorf("failed to open TLS connection: %w", err)
		}
	}
	if err != nil {
		Error("Failed to open connection", "url", ExtractWithoutPassword(address), "error", err, "ID", connOptions.Id)
		return fmt.Errorf("failed to open connection: %w", err)
	}
	a.properties = azureConnection.Properties()
	err = a.featuresAvailable.ParseProperties(a.properties)
	if err != nil {
		Warn("Validate properties Error.", ExtractWithoutPassword(address), err)
	}

	if !a.featuresAvailable.is4OrMore {
		Warn("The server version is less than 4.0.0", ExtractWithoutPassword(address), "ID", connOptions.Id)
	}

	if !a.featuresAvailable.isRabbitMQ {
		Warn("The server is not RabbitMQ", ExtractWithoutPassword(address))
	}

	Debug("Connected to", ExtractWithoutPassword(address), "ID", connOptions.Id)
	a.azureConnection = azureConnection
	a.session, err = a.azureConnection.NewSession(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to open session, for the connection id:%s, error: %w", a.Id(), err)
	}
	go func() {
		<-azureConnection.Done()
		{
			a.lifeCycle.SetState(&StateClosed{error: azureConnection.Err()})
			if azureConnection.Err() != nil {
				Error("connection closed unexpectedly", "error", azureConnection.Err(), "ID", a.Id())
				a.maybeReconnect()

				return
			}
			Debug("connection closed successfully", "ID", a.Id())
		}

	}()

	err = a.management.Open(ctx, a)
	if err != nil {
		// TODO close connection?
		return err
	}
	Debug("Management interface opened", "ID", a.Id())

	return nil
}
func (a *AmqpConnection) maybeReconnect() {
	if !a.amqpConnOptions.RecoveryConfiguration.ActiveRecovery {
		Info("Recovery is disabled, closing connection", "ID", a.Id())
		return
	}
	a.lifeCycle.SetState(&StateReconnecting{})
	// Add exponential backoff with jitter
	baseDelay := a.amqpConnOptions.RecoveryConfiguration.BackOffReconnectInterval
	maxDelay := 1 * time.Minute

	for attempt := 1; attempt <= a.amqpConnOptions.RecoveryConfiguration.MaxReconnectAttempts; attempt++ {
		///wait for before reconnecting
		// add some random milliseconds to the wait time to avoid thundering herd
		// the random time is between 0 and 500 milliseconds
		// Calculate delay with exponential backoff and jitter
		jitter := time.Duration(rand.Intn(500)) * time.Millisecond
		delay := baseDelay + jitter
		if delay > maxDelay {
			delay = maxDelay
		}

		Info("Attempting reconnection", "attempt", attempt, "delay", delay, "ID", a.Id())
		time.Sleep(delay)
		// context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		// try to createSender
		err := a.open(ctx, a.address, a.amqpConnOptions)
		cancel()

		if err == nil {
			a.recoverTopology()
			a.restartEntities()
			a.lifeCycle.SetState(&StateOpen{})
			return
		}

		if errors.Is(err, ErrConnectionClosed) {
			Info("Connection was closed during reconnect, aborting.", "ID", a.Id())
			return
		}

		baseDelay *= 2
		Error("Reconnection attempt failed", "attempt", attempt, "error", err, "ID", a.Id())
	}

	// If we reach here, all attempts failed
	Error("All reconnection attempts failed, closing connection", "ID", a.Id())
	a.lifeCycle.SetState(&StateClosed{error: ErrMaxReconnectAttemptsReached})

}

// restartEntities attempts to restart all publishers and consumers after a reconnection
func (a *AmqpConnection) restartEntities() {
	var publisherFails, consumerFails int32

	// Restart publishers
	a.entitiesTracker.publishers.Range(func(key, value any) bool {
		publisher := value.(*Publisher)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := publisher.createSender(ctx); err != nil {
			atomic.AddInt32(&publisherFails, 1)
			Error("Failed to restart publisher", "ID", publisher.Id(), "error", err, "ID", a.Id())
		}
		return true
	})

	// Restart consumers
	a.entitiesTracker.consumers.Range(func(key, value any) bool {
		consumer := value.(*Consumer)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		if err := consumer.createReceiver(ctx); err != nil {
			atomic.AddInt32(&consumerFails, 1)
			Error("Failed to restart consumer", "ID", consumer.Id(), "error", err, "ID", a.Id())
		}
		return true
	})

	Info("Entity restart complete",
		"publisherFails", publisherFails,
		"consumerFails", consumerFails)
}

func (a *AmqpConnection) recoverTopology() {
	Debug("Recovering topology")
	// Set the isRecovering flag to prevent duplicate recovery records.
	// Using atomic operations since this runs in the recovery goroutine
	// while public API methods can be called from user goroutines.
	a.management.isRecovering.Store(true)
	defer func() {
		a.management.isRecovering.Store(false)
	}()

	for _, exchange := range a.topologyRecoveryRecords.exchanges {
		Debug("Recovering exchange", "exchange", exchange.exchangeName)
		_, err := a.Management().DeclareExchange(context.Background(), exchange.toIExchangeSpecification())
		if err != nil {
			Error("Failed to recover exchange", "error", err, "ID", a.Id(), "exchange", exchange.exchangeName)
		}
	}
	for _, queue := range a.topologyRecoveryRecords.queues {
		Debug("Recovering queue", "queue", queue.queueName)
		_, err := a.Management().DeclareQueue(context.Background(), queue.toIQueueSpecification())
		if err != nil {
			Error("Failed to recover queue", "error", err, "ID", a.Id(), "queue", queue.queueName)
		}
	}
	for _, binding := range a.topologyRecoveryRecords.bindings {
		Debug("Recovering binding", "bind source", binding.sourceExchange, "bind destination", binding.destination)
		_, err := a.Management().Bind(context.Background(), binding.toIBindingSpecification())
		if err != nil {
			Error("Failed to recover binding", "error", err, "ID", a.Id(), "bind source", binding.sourceExchange, "bind destination", binding.destination)
		}
	}
}

func (a *AmqpConnection) close() {
	if a.refMap != nil {
		a.refMap.Delete(a.Id())
	}
	a.entitiesTracker.CleanUp()
}

/*
Close closes the connection to the AMQP 1.0 server and the management interface.
All the publishers and consumers are closed as well.
*/
func (a *AmqpConnection) Close(ctx context.Context) error {
	a.mutex.Lock()
	if a.closed {
		a.mutex.Unlock()
		return nil
	}
	a.closed = true
	defer a.mutex.Unlock()
	// the status closed (lifeCycle.SetState(&StateClosed{error: nil})) is not set here
	// it is set in the connection.Done() channel
	// the channel is called anyway
	// see the open(...) function with a.lifeCycle.SetState(&StateClosed{error: connection.Err()})

	err := a.management.Close(ctx)
	if err != nil {
		Error("Failed to close management", "error:", err, "ID", a.Id())
	}
	err = a.azureConnection.Close()
	a.close()

	// Record the connection closing metric
	a.metricsCollector.CloseConnection()

	return err
}

// NotifyStatusChange registers a channel to receive getState change notifications
// from the connection.
func (a *AmqpConnection) NotifyStatusChange(channel chan *StateChanged) {
	a.lifeCycle.notifyStatusChange(channel)
}

func (a *AmqpConnection) State() ILifeCycleState {
	return a.lifeCycle.State()
}

func (a *AmqpConnection) Id() string {
	return a.amqpConnOptions.Id
}

// *** management section ***

// Management returns the management interface for the connection.
// The management interface is used to declare and delete exchanges, queues, and bindings.
func (a *AmqpConnection) Management() *AmqpManagement {
	return a.management
}

func (a *AmqpConnection) RefreshToken(background context.Context, token string) error {
	if !a.amqpConnOptions.isOAuth2() {
		return fmt.Errorf("the connection is not configured to use OAuth2 token")
	}

	if a.amqpConnOptions.isOAuth2() && !a.featuresAvailable.is41OrMore {
		return fmt.Errorf("the server does not support OAuth2 token, you need to upgrade to RabbitMQ 4.1 or later")
	}

	err := a.Management().refreshToken(background, token)
	if err != nil {
		return err
	}
	// update the SASLType in case of reconnect after token refresh
	// it should use the new token
	a.amqpConnOptions.SASLType = amqp.SASLTypePlain("", token)
	return nil

}

//*** end management section ***

// sanitizeWebSocketURL ensures the URL is correctly formatted for the Gorilla websocket dialer.
func sanitizeWebSocketURL(rawURL string) (string, http.Header, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, err
	}

	if u.Scheme != "ws" && u.Scheme != "wss" {
		return "", nil, fmt.Errorf("invalid websocket scheme: %s", u.Scheme)
	}

	// Prepare Headers for Auth
	headers := http.Header{}
	// add sec-websocket-protocol amqp
	// mandatory for AMQP over WebSocket
	// https://www.rfc-editor.org/rfc/rfc6455.html
	headers.Add("Sec-WebSocket-Protocol", "amqp")

	if u.User != nil {
		username := u.User.Username()
		password, _ := u.User.Password()

		// Construct Basic Auth Header manually
		auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
		headers.Add("Authorization", "Basic "+auth)
		u.User = nil
	}

	if u.Path == "" {
		u.Path = "/"
	} else if u.Path[0] != '/' {
		u.Path = "/" + u.Path
	}

	return u.String(), headers, nil
}
