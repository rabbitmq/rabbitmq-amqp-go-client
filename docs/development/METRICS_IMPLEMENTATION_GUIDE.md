# RabbitMQ AMQP Client Metrics Implementation Guide

## Document Purpose

This document provides code-agnostic guidelines for implementing metrics collection in RabbitMQ AMQP client libraries. It is based on the architecture and patterns used in the Java client implementation and is intended to guide implementations in other programming languages.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Concepts](#core-concepts)
3. [Metrics Collection Interface](#metrics-collection-interface)
4. [Metric Types and Categories](#metric-types-and-categories)
5. [Integration Points](#integration-points)
6. [Implementation Patterns](#implementation-patterns)
7. [Concrete Implementation Guidelines](#concrete-implementation-guidelines)
8. [Testing Considerations](#testing-considerations)
9. [Best Practices](#best-practices)
10. [OpenTelemetry Semantic Conventions for RabbitMQ](#opentelemetry-semantic-conventions-for-rabbitmq)

---

## 1. Architecture Overview

### 1.1 Design Philosophy

The metrics system follows these core principles:

- **Optional and Pluggable**: Metrics collection is completely optional; a no-operation implementation is provided by default
- **Non-Intrusive**: Metrics collection does not affect the core client functionality
- **Framework Agnostic**: The interface is decoupled from specific metrics frameworks
- **Single Responsibility**: Each component knows when to trigger metrics collection, but delegates the actual collection logic

### 1.2 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Application                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ (1) Provides MetricsCollector
                         │     during Environment setup
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Environment                             │
│  - Holds reference to MetricsCollector                       │
│  - Passes it to all Connections                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ (2) Passes MetricsCollector reference
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                      Connection                              │
│  - Calls metricsCollector.openConnection()                   │
│  - Calls metricsCollector.closeConnection()                  │
│  - Passes reference to Publishers and Consumers              │
│  - Sets up byte counting hooks in transport layer            │
└─────────────┬───────────────────────────────────────────────┘
              │
              │ (3) Passes reference to child resources
              │
       ┌──────┴────────┐
       │               │
       ▼               ▼
┌─────────────┐  ┌────────────┐
│  Publisher  │  │  Consumer  │
│             │  │            │
└─────────────┘  └────────────┘
```

---

## 2. Core Concepts

### 2.1 MetricsCollector Interface

The central abstraction is the **MetricsCollector** interface, which defines callback methods for all measurable events in the client lifecycle. This interface serves as a contract between the client library and any metrics framework.

**Key Characteristics**:
- All methods are void (fire-and-forget)
- Methods are non-blocking
- No exceptions should escape to the caller
- Methods are called synchronously at the point of the event

### 2.2 No-Operation Implementation

A **NoOpMetricsCollector** implementation must be provided that does nothing. This serves as:
- The default metrics collector when none is specified
- A null-object pattern to avoid conditional checks
- A performance baseline (zero overhead when metrics are disabled)

### 2.3 Injection Point

The metrics collector is injected at the **Environment** (or equivalent top-level container) level during initialization. This ensures:
- Centralized configuration
- Consistent metrics across all connections
- Single point of configuration for the entire client

---

## 3. Metrics Collection Interface

### 3.1 Complete Method Signature Specification

The MetricsCollector interface must expose the following methods:

#### Connection Lifecycle
```
openConnection()
  - Called when: A connection to the broker is successfully established
  - Call frequency: Once per connection
  - Thread context: Connection initialization thread
  
closeConnection()
  - Called when: A connection is permanently closed
  - Call frequency: Once per connection
  - Thread context: Connection closing thread
  - Note: NOT called during connection recovery
```

#### Publisher Lifecycle
```
openPublisher()
  - Called when: A publisher resource is successfully created
  - Call frequency: Once per publisher
  - Thread context: Publisher initialization thread
  
closePublisher()
  - Called when: A publisher is closed/destroyed
  - Call frequency: Once per publisher
  - Thread context: Publisher closing thread
```

#### Consumer Lifecycle
```
openConsumer()
  - Called when: A consumer resource is successfully created
  - Call frequency: Once per consumer
  - Thread context: Consumer initialization thread
  
closeConsumer()
  - Called when: A consumer is closed/destroyed
  - Call frequency: Once per consumer
  - Thread context: Consumer closing thread
```

#### Message Publishing
```
publish()
  - Called when: A message is successfully sent to the broker
  - Call frequency: Once per message
  - Thread context: Publishing thread (may be async)
  - Timing: Called immediately after sending, before broker acknowledgment
  
publishDisposition(disposition: PublishDisposition)
  - Called when: The broker acknowledges a published message
  - Parameter: Enum with values: ACCEPTED, REJECTED, RELEASED
  - Call frequency: Once per published message
  - Thread context: Async callback thread
  - Timing: Called after broker settles the delivery
```

**PublishDisposition Values**:
- **ACCEPTED**: Message was accepted by the broker and will be routed
- **REJECTED**: Message was rejected by the broker (e.g., validation failed)
- **RELEASED**: Message was released by the broker (e.g., couldn't route, no queue)

#### Message Consumption
```
consume()
  - Called when: A message is delivered to a consumer
  - Call frequency: Once per delivered message
  - Thread context: Message delivery thread
  - Timing: Called before invoking the application's message handler
  
consumeDisposition(disposition: ConsumeDisposition)
  - Called when: The consumer settles (acknowledges) a message
  - Parameter: Enum with values: ACCEPTED, DISCARDED, REQUEUED
  - Call frequency: Once per consumed message (if not pre-settled)
  - Thread context: Message handler or settlement thread
```

**ConsumeDisposition Values**:
- **ACCEPTED**: Consumer accepted and processed the message
- **DISCARDED**: Consumer discarded/rejected the message (dead letter)
- **REQUEUED**: Consumer requeued the message for redelivery

#### Network I/O
```
writtenBytes(byteCount: int)
  - Called when: Data is written to the network socket
  - Parameter: Number of bytes written in this operation
  - Call frequency: Many times per second (high frequency)
  - Thread context: Network I/O thread
  - Note: Cumulative - caller must aggregate
  
readBytes(byteCount: int)
  - Called when: Data is read from the network socket
  - Parameter: Number of bytes read in this operation
  - Call frequency: Many times per second (high frequency)
  - Thread context: Network I/O thread
  - Note: Cumulative - caller must aggregate
```

---

## 4. Metric Types and Categories

### 4.1 Gauge Metrics (Current State)

These metrics represent the **current value** at any point in time:

| Metric | Type | Meaning | Modification Pattern |
|--------|------|---------|---------------------|
| connections | Gauge | Number of currently open connections | Increment on open, decrement on close |
| publishers | Gauge | Number of currently open publishers | Increment on open, decrement on close |
| consumers | Gauge | Number of currently open consumers | Increment on open, decrement on close |

**Implementation Notes**:
- Use atomic counters or similar thread-safe mechanisms
- Start at 0
- Can go negative temporarily if implementation has race conditions (fix these)
- Should never be negative in a correct implementation

### 4.2 Counter Metrics (Cumulative Totals)

These metrics represent **cumulative totals** that only increase:

| Metric | Type | Meaning | Unit |
|--------|------|---------|------|
| published | Counter | Total number of messages published | messages |
| published_accepted | Counter | Total messages accepted by broker | messages |
| published_rejected | Counter | Total messages rejected by broker | messages |
| published_released | Counter | Total messages released by broker | messages |
| consumed | Counter | Total number of messages delivered to consumers | messages |
| consumed_accepted | Counter | Total messages accepted by consumers | messages |
| consumed_discarded | Counter | Total messages discarded by consumers | messages |
| consumed_requeued | Counter | Total messages requeued by consumers | messages |
| written_bytes | Counter | Total bytes written to network | bytes |
| read_bytes | Counter | Total bytes read from network | bytes |

**Implementation Notes**:
- These should monotonically increase
- Never reset during the lifetime of the application
- Use thread-safe increment operations
- For byte metrics, expect very high frequency updates

### 4.3 Metric Naming Convention

Recommended naming pattern: `{prefix}.{metric_name}`

Example with prefix "rabbitmq.amqp":
- `rabbitmq.amqp.connections`
- `rabbitmq.amqp.published`
- `rabbitmq.amqp.consumed_accepted`
- `rabbitmq.amqp.written_bytes`

---

## 5. Integration Points

### 5.1 Environment/Client Setup

**When**: During client initialization  
**What**: Inject the MetricsCollector implementation

```
Pseudocode:
  environment = EnvironmentBuilder()
    .metricsCollector(metricsCollectorInstance)
    .build()
```

**Key Points**:
- This is the ONLY place where the metrics collector is specified
- Default to NoOpMetricsCollector if none provided
- Store reference in the environment for all connections to access

### 5.2 Connection Lifecycle

#### Opening a Connection

**Location**: After successful connection establishment, before returning to caller

```
Pseudocode:
  function openConnection():
    nativeConnection = establishConnection()
    validateConnection(nativeConnection)
    setState(OPEN)
    environment.metricsCollector.openConnection()  // <-- HERE
    return this
```

#### Closing a Connection

**Location**: After closing underlying resources, during final cleanup

```
Pseudocode:
  function close():
    if alreadyClosed:
      return
    setState(CLOSING)
    closeChildren()  // publishers, consumers
    closeNativeConnection()
    setState(CLOSED)
    environment.metricsCollector.closeConnection()  // <-- HERE
```

**Important**: Do NOT call `closeConnection()` during connection recovery. Recovery means the connection is temporarily unavailable but will reconnect. Only call when permanently closed.

### 5.3 Publisher Lifecycle

#### Opening a Publisher

**Location**: After successful publisher creation

```
Pseudocode:
  function createPublisher(config):
    nativeSender = session.createSender(config.address)
    nativeSender.open()
    setState(OPEN)
    metricsCollector.openPublisher()  // <-- HERE
    return publisher
```

#### Publishing a Message

**Location**: Two distinct points

```
Pseudocode:
  function publish(message, callback):
    tracker = sender.send(message)  
    metricsCollector.publish()  // <-- POINT 1: Immediately after send
    
    tracker.onSettlement = (disposition) => {
      status = mapDisposition(disposition)
      metricsCollector.publishDisposition(status)  // <-- POINT 2: On broker response
      callback.onComplete(status)
    }
```

#### Closing a Publisher

**Location**: After closing the underlying sender

```
Pseudocode:
  function close():
    if alreadyClosed:
      return
    setState(CLOSING)
    nativeSender.close()
    connection.removePublisher(this)
    setState(CLOSED)
    metricsCollector.closePublisher()  // <-- HERE
```

### 5.4 Consumer Lifecycle

#### Opening a Consumer

**Location**: After successful consumer creation

```
Pseudocode:
  function createConsumer(config):
    nativeReceiver = session.createReceiver(config.queue)
    nativeReceiver.setHandler(internalHandler)
    nativeReceiver.open()
    nativeReceiver.addCredits(initialCredits)
    setState(OPEN)
    metricsCollector.openConsumer()  // <-- HERE
    return consumer
```

#### Consuming a Message

**Location**: In the message delivery handler, before user handler

```
Pseudocode:
  function internalMessageHandler(nativeDelivery):
    metricsCollector.consume()  // <-- POINT 1: Before user handler
    
    message = decodeMessage(nativeDelivery)
    context = createContext(nativeDelivery)
    
    try:
      userHandler.handle(context, message)
    catch error:
      context.discard()  // This will trigger disposition metric
```

#### Settling a Message

**Location**: In the context settlement methods

```
Pseudocode:
  class MessageContext:
    function accept():
      if not alreadySettled:
        nativeDelivery.settle(ACCEPTED)
        metricsCollector.consumeDisposition(ACCEPTED)  // <-- HERE
        
    function discard():
      if not alreadySettled:
        nativeDelivery.settle(REJECTED)
        metricsCollector.consumeDisposition(DISCARDED)  // <-- HERE
        
    function requeue():
      if not alreadySettled:
        nativeDelivery.settle(RELEASED)
        metricsCollector.consumeDisposition(REQUEUED)  // <-- HERE
```

**Special Case - Pre-Settled/Auto-Ack Mode**: If the consumer is in auto-acknowledge mode, the message is settled automatically by the protocol layer. In this case:
- Call `consume()` normally
- Immediately call `consumeDisposition(ACCEPTED)` without waiting
- User handler cannot call settlement methods (throw exception if attempted)

#### Closing a Consumer

**Location**: After closing the underlying receiver

```
Pseudocode:
  function close():
    if alreadyClosed:
      return
    setState(CLOSING)
    nativeReceiver.close()
    connection.removeConsumer(this)
    setState(CLOSED)
    metricsCollector.closeConsumer()  // <-- HERE
```

### 5.5 Network I/O Tracking

This is the most framework-dependent integration point.

#### General Pattern

The byte tracking hooks must be installed at the **transport layer** (socket/network level), not at the application protocol level.

#### Approach 1: Transport Options (Callback-Based)

```
Pseudocode:
  // In Environment initialization:
  readBytesCallback = (count) => metricsCollector.readBytes(count)
  writtenBytesCallback = (count) => metricsCollector.writtenBytes(count)
  
  // In Connection creation:
  transportOptions = TransportOptions()
    .readBytesCallback(readBytesCallback)
    .writtenBytesCallback(writtenBytesCallback)
  
  connection = createConnection(address, transportOptions)
```

#### Approach 2: Transport Wrapper (Decorator Pattern)

```
Pseudocode:
  class MetricsTrackingSocket extends Socket:
    function write(buffer):
      bytesWritten = super.write(buffer)
      metricsCollector.writtenBytes(bytesWritten)
      return bytesWritten
      
    function read(buffer):
      bytesRead = super.read(buffer)
      metricsCollector.readBytes(bytesRead)
      return bytesRead
```

#### What to Measure

- **Include**: All bytes on the wire (AMQP frames, SASL, TLS payload)
- **Exclude**: TLS overhead/headers (measure application data, not encryption overhead)
- **Timing**: Measure actual I/O, not buffer operations

#### Performance Considerations

Byte tracking is **high frequency** - can be called thousands of times per second. Implementations should:
- Use very fast atomic operations
- Avoid locks if possible
- Consider buffering/batching if the metrics backend is slow
- Make the callback as lightweight as possible

---

## 6. Implementation Patterns

### 6.1 Concrete Implementation Structure

A typical concrete implementation (e.g., Micrometer, Prometheus, StatsD) should:

#### Store Metric Instruments

```
Pseudocode:
  class MicrometerMetricsCollector implements MetricsCollector:
    // Gauge instruments (up/down counters)
    private connectionsGauge
    private publishersGauge
    private consumersGauge
    
    // Counter instruments (monotonic increase)
    private publishedCounter
    private publishedAcceptedCounter
    private publishedRejectedCounter
    private publishedReleasedCounter
    private consumedCounter
    private consumedAcceptedCounter
    private consumedRequeuedCounter
    private consumedDiscardedCounter
    private writtenBytesCounter
    private readBytesCounter
```

#### Initialize in Constructor

```
Pseudocode:
  constructor(registry, prefix, tags):
    connectionsGauge = registry.gauge(prefix + ".connections", tags)
    publishersGauge = registry.gauge(prefix + ".publishers", tags)
    // ... etc
```

#### Implement Interface Methods

```
Pseudocode:
  function openConnection():
    connectionsGauge.increment()
    
  function closeConnection():
    connectionsGauge.decrement()
    
  function publish():
    publishedCounter.increment()
    
  function publishDisposition(disposition):
    switch disposition:
      case ACCEPTED: publishedAcceptedCounter.increment()
      case REJECTED: publishedRejectedCounter.increment()
      case RELEASED: publishedReleasedCounter.increment()
  
  // ... similar for all other methods
```

### 6.2 No-Operation Implementation

```
Pseudocode:
  class NoOpMetricsCollector implements MetricsCollector:
    // Singleton instance
    static INSTANCE = new NoOpMetricsCollector()
    
    private constructor()  // Prevent external instantiation
    
    // All methods are empty
    function openConnection(): pass
    function closeConnection(): pass
    function openPublisher(): pass
    // ... etc (all empty)
```

### 6.3 Tagging/Labeling Support

Many metrics systems support tags/labels for dimensionality:

```
Pseudocode:
  constructor(registry, prefix, tags):
    // tags might be: { "application": "my-app", "environment": "prod" }
    this.tags = tags
    
    // Apply tags to all metrics
    connectionsGauge = registry.gauge(
      name: prefix + ".connections",
      tags: this.tags
    )
```

Common useful tags:
- Application name
- Environment (dev/staging/prod)
- Host/instance identifier
- Custom user-defined tags

**Note**: Tags should be set at initialization and remain constant. Do not use dynamic tags per-operation (high cardinality problem).

---

## 7. Concrete Implementation Guidelines

### 7.1 Thread Safety

All MetricsCollector implementations must be **thread-safe**:
- Methods can be called from multiple threads simultaneously
- Use atomic operations for counters
- Avoid locks in the hot path (byte counting)
- Consider thread-local buffering for high-frequency metrics

### 7.2 Error Handling

Metrics collection should **never** throw exceptions to the caller:

```
Pseudocode:
  function publish():
    try:
      publishedCounter.increment()
    catch error:
      // Log error but don't propagate
      logger.warn("Failed to record publish metric", error)
```

### 7.3 Performance Guidelines

- **Gauge updates**: Fast atomic increment/decrement (< 100 nanoseconds)
- **Counter increments**: Fast atomic increment (< 100 nanoseconds)
- **Byte counting**: Ultra-fast (< 50 nanoseconds per call)
- If the metrics backend is slow, use buffering/batching
- Consider async/background reporting to avoid blocking

### 7.4 Metrics Framework Integration

#### Micrometer (JVM)

```
Pseudocode:
  import MeterRegistry
  import Counter
  import Gauge
  
  class MicrometerMetricsCollector:
    constructor(registry: MeterRegistry, prefix: String):
      this.connectionsGauge = new AtomicLong(0)
      registry.gauge(prefix + ".connections", this.connectionsGauge)
      
      this.publishedCounter = Counter.builder(prefix + ".published")
        .register(registry)
```

#### Prometheus (Direct)

```
Pseudocode:
  import prometheus
  
  class PrometheusMetricsCollector:
    constructor(prefix: String):
      this.connectionsGauge = prometheus.Gauge(
        name: prefix + "_connections",
        documentation: "Current number of open connections"
      )
      
      this.publishedCounter = prometheus.Counter(
        name: prefix + "_published_total",
        documentation: "Total published messages"
      )
```

#### StatsD

```
Pseudocode:
  import statsd
  
  class StatsDMetricsCollector:
    constructor(client: StatsDClient, prefix: String):
      this.client = client
      this.prefix = prefix
      
    function openConnection():
      client.increment(prefix + ".connections", delta: 1)
      
    function closeConnection():
      client.increment(prefix + ".connections", delta: -1)
```

### 7.5 Custom Implementation Example

Here's a complete minimal example:

```
Pseudocode:
  class SimpleMetricsCollector implements MetricsCollector:
    private connections: AtomicInteger = 0
    private publishers: AtomicInteger = 0
    private consumers: AtomicInteger = 0
    private published: AtomicLong = 0
    private consumed: AtomicLong = 0
    private writtenBytes: AtomicLong = 0
    private readBytes: AtomicLong = 0
    
    function openConnection():
      connections.incrementAndGet()
      
    function closeConnection():
      connections.decrementAndGet()
      
    function openPublisher():
      publishers.incrementAndGet()
      
    function closePublisher():
      publishers.decrementAndGet()
      
    function openConsumer():
      consumers.incrementAndGet()
      
    function closeConsumer():
      consumers.decrementAndGet()
      
    function publish():
      published.incrementAndGet()
      
    function publishDisposition(disposition):
      // Optional: track dispositions separately
      pass
      
    function consume():
      consumed.incrementAndGet()
      
    function consumeDisposition(disposition):
      // Optional: track dispositions separately
      pass
      
    function writtenBytes(count: int):
      writtenBytes.addAndGet(count)
      
    function readBytes(count: int):
      readBytes.addAndGet(count)
      
    // Accessor methods for reporting
    function getMetrics():
      return {
        "connections": connections.get(),
        "publishers": publishers.get(),
        "consumers": consumers.get(),
        "published": published.get(),
        "consumed": consumed.get(),
        "written_bytes": writtenBytes.get(),
        "read_bytes": readBytes.get()
      }
```

---

## 8. Testing Considerations

### 8.1 Unit Testing Metrics Collection

Create a test/mock implementation that records calls:

```
Pseudocode:
  class RecordingMetricsCollector implements MetricsCollector:
    callLog = []
    
    function openConnection():
      callLog.append({"method": "openConnection", "timestamp": now()})
      
    function publish():
      callLog.append({"method": "publish", "timestamp": now()})
    
    // ... etc
    
    // Test helper methods
    function assertCallCount(method: String, expectedCount: int):
      actualCount = callLog.count(call => call.method == method)
      assert actualCount == expectedCount
```

### 8.2 Integration Test Scenarios

Test the following scenarios:

1. **Connection lifecycle**:
   - Open connection → verify openConnection() called once
   - Close connection → verify closeConnection() called once
   - Recovery scenario → verify closeConnection() NOT called

2. **Publisher lifecycle**:
   - Create publisher → verify openPublisher() called
   - Publish messages → verify publish() called for each
   - Receive dispositions → verify publishDisposition() called for each
   - Close publisher → verify closePublisher() called

3. **Consumer lifecycle**:
   - Create consumer → verify openConsumer() called
   - Consume messages → verify consume() called for each delivery
   - Settle messages → verify consumeDisposition() called with correct disposition
   - Close consumer → verify closeConsumer() called

4. **Pre-settled consumers**:
   - Consume message → verify both consume() and consumeDisposition(ACCEPTED) called immediately

5. **Byte counting**:
   - Send messages → verify writtenBytes() called with positive values
   - Receive messages → verify readBytes() called with positive values

6. **Metric accuracy**:
   - Open 3 connections → verify gauge shows 3
   - Close 1 connection → verify gauge shows 2
   - Publish 100 messages, receive 100 accepts → verify counters match

### 8.3 Performance Testing

Verify that metrics collection doesn't significantly impact performance:

```
Pseudocode:
  function benchmarkPublishWithMetrics():
    collector = MicrometerMetricsCollector(registry)
    environment = Environment(metricsCollector: collector)
    
    startTime = now()
    for i in 1..100000:
      publisher.publish(message)
    duration = now() - startTime
    
  function benchmarkPublishWithoutMetrics():
    environment = Environment(metricsCollector: NoOpMetricsCollector.INSTANCE)
    
    startTime = now()
    for i in 1..100000:
      publisher.publish(message)
    duration = now() - startTime
    
  // Overhead should be < 5%
  assert (withMetricsDuration - withoutMetricsDuration) / withoutMetricsDuration < 0.05
```

---

## 9. Best Practices

### 9.1 Configuration

- Provide metrics collection as an opt-in feature
- Default to NoOpMetricsCollector for zero overhead
- Allow prefix/namespace customization
- Support tag/label configuration
- Document required dependencies (e.g., Micrometer, Prometheus client)

### 9.2 Documentation

User documentation should include:

1. **Setup Example**:
   ```
   How to enable metrics with popular frameworks
   ```

2. **Available Metrics**:
   - List all metric names
   - Describe what each metric measures
   - Specify metric type (gauge/counter)
   - Provide expected value ranges

3. **Dashboard Examples**:
   - Provide sample Grafana dashboards
   - Include example PromQL queries
   - Show visualization best practices

4. **Performance Impact**:
   - Document expected overhead (should be minimal)
   - Provide guidance on high-throughput scenarios

### 9.3 Metric Naming

Follow these conventions:

1. **Consistency**: Use the same prefix for all metrics
2. **Clarity**: Names should be self-explanatory
3. **Units**: Include units in name when ambiguous (e.g., `_bytes`, `_seconds`)
4. **Framework conventions**: Follow target framework naming (e.g., Prometheus uses `_total` suffix for counters)

Examples:
- Good: `rabbitmq_amqp_connections`, `rabbitmq_amqp_published_total`
- Bad: `conn`, `msgs`, `pub`

### 9.4 Backwards Compatibility

When evolving the metrics interface:

1. **Additive changes**: New methods can be added (with default no-op implementations)
2. **Deprecation**: Don't remove methods; deprecate first
3. **Semantic stability**: Don't change the meaning of existing metrics
4. **Documentation**: Clearly document any changes in behavior

### 9.5 Resource Cleanup

- Metrics should be cleaned up when the environment is closed
- Unregister metrics from the registry if the framework requires it
- Avoid metric leaks in long-running applications

### 9.6 Common Pitfalls to Avoid

1. **Don't block**: Never perform slow operations in metric methods
2. **Don't throw**: Catch and log all exceptions
3. **Don't use locks**: Use lock-free atomic operations
4. **Don't create per-message objects**: Reuse metric instances
5. **Don't forget thread safety**: All methods must be thread-safe
6. **Don't measure too much**: Focus on useful metrics, not everything
7. **Don't call metrics during recovery**: Recovery is not a close/open cycle

---

## 10. OpenTelemetry Semantic Conventions for RabbitMQ

When implementing metrics using OpenTelemetry, follow the [OTEL Semantic Conventions for RabbitMQ](https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/). These conventions ensure consistent telemetry across different RabbitMQ client implementations.

### 10.1 Required Attributes

The following attributes are required for RabbitMQ messaging operations:

| Attribute | Description | Example Values |
|-----------|-------------|----------------|
| `messaging.system` | MUST be set to `"rabbitmq"` | `rabbitmq` |
| `messaging.destination.name` | The message destination name (see below) | `direct_logs:warning`, `logs` |
| `messaging.operation.name` | The system-specific name of the messaging operation | `ack`, `nack`, `send`, `receive`, `requeue` |

### 10.2 Conditionally Required Attributes

| Attribute | Condition | Description | Example Values |
|-----------|-----------|-------------|----------------|
| `error.type` | If operation failed | Describes the class of error | `amqp:decode-error`, `channel-error` |
| `messaging.operation.type` | If applicable | Type of messaging operation | `create`, `send`, `receive`, `settle` |
| `messaging.rabbitmq.destination.routing_key` | If not empty | RabbitMQ message routing key | `myKey` |
| `messaging.rabbitmq.message.delivery_tag` | When available | RabbitMQ message delivery tag | `123` |
| `server.address` | If available | Server domain name or IP address | `example.com`, `10.1.2.80` |

### 10.3 Recommended Attributes

| Attribute | Description | Example Values |
|-----------|-------------|----------------|
| `messaging.message.conversation_id` | Message correlation ID property | `MyConversationId` |
| `messaging.message.id` | Message identifier as a string | `452a7c7c7c7048c2f887f61572b18fc2` |
| `network.peer.address` | Peer address of the network connection | `10.1.2.80`, `/tmp/my.sock` |
| `network.peer.port` | Peer port number of the network connection | `65123` |
| `server.port` | Server port number | `80`, `8080`, `443`, `5672` |

### 10.4 Optional Attributes

| Attribute | Description | Example Values |
|-----------|-------------|----------------|
| `messaging.message.body.size` | The size of the message body in bytes | `1439` |

### 10.5 Destination Name Construction

The `messaging.destination.name` attribute should be constructed as follows:

**On the producer side:**
- `{exchange}:{routing_key}` when both values are present and non-empty
- `{exchange}` when only the exchange is available
- `{routing_key}` when only the routing key is available
- `amq.default` when the default exchange is used and no routing key is provided

**On the consumer side:**
- `{exchange}:{routing_key}:{queue}` when all values are present and non-empty
- If any value is empty (e.g., the default exchange), it should be omitted
- For cases when `{routing_key}` and `{queue}` are equal, only one should be used

### 10.6 Operation Types

The `messaging.operation.type` attribute uses the following well-known values:

| Value | Description |
|-------|-------------|
| `create` | A message is created (used for batch sending scenarios) |
| `send` | One or more messages are provided for sending |
| `receive` | One or more messages are requested by a consumer |
| `process` | One or more messages are processed by a consumer |
| `settle` | One or more messages are settled (acknowledged) |

### 10.7 Operation Names

The `messaging.operation.name` attribute should use descriptive names:

| Operation | Value |
|-----------|-------|
| Sending a message | `send` |
| Receiving a message | `receive` |
| Accepting/acknowledging a message | `ack` |
| Rejecting/discarding a message | `nack` |
| Requeuing a message | `requeue` |

### 10.8 Implementation Example

When recording metrics with OTEL semantic conventions, include the appropriate attributes:

```
Pseudocode:
  function recordPublishMetric(message, destination):
    attributes = {
      "messaging.system": "rabbitmq",
      "messaging.destination.name": buildDestinationName(exchange, routingKey),
      "messaging.operation.name": "send",
      "messaging.operation.type": "send",
      "server.address": connectionHost,
      "server.port": connectionPort
    }
    
    if routingKey != "":
      attributes["messaging.rabbitmq.destination.routing_key"] = routingKey
    
    if message.messageId != "":
      attributes["messaging.message.id"] = message.messageId
    
    publishedCounter.add(1, attributes)
    
  function recordConsumeMetric(message, queue):
    attributes = {
      "messaging.system": "rabbitmq",
      "messaging.destination.name": queue,
      "messaging.operation.name": "receive",
      "messaging.operation.type": "receive",
      "server.address": connectionHost,
      "server.port": connectionPort
    }
    
    if message.messageId != "":
      attributes["messaging.message.id"] = message.messageId
    
    consumedCounter.add(1, attributes)
    
  function recordDispositionMetric(disposition, destination):
    attributes = {
      "messaging.system": "rabbitmq",
      "messaging.destination.name": destination,
      "messaging.operation.type": "settle"
    }
    
    switch disposition:
      case ACCEPTED:
        attributes["messaging.operation.name"] = "ack"
      case DISCARDED:
        attributes["messaging.operation.name"] = "nack"
      case REQUEUED:
        attributes["messaging.operation.name"] = "requeue"
    
    dispositionCounter.add(1, attributes)
```

### 10.9 Go Implementation with OpenTelemetry

For Go implementations using OpenTelemetry, define semantic convention attribute keys:

```go
import "go.opentelemetry.io/otel/attribute"

const (
    // Required attributes
    MessagingSystemKey          = attribute.Key("messaging.system")
    MessagingDestinationNameKey = attribute.Key("messaging.destination.name")
    MessagingOperationNameKey   = attribute.Key("messaging.operation.name")
    MessagingOperationTypeKey   = attribute.Key("messaging.operation.type")

    // Server attributes
    ServerAddressKey = attribute.Key("server.address")
    ServerPortKey    = attribute.Key("server.port")

    // RabbitMQ-specific attributes
    MessagingRabbitMQRoutingKeyKey = attribute.Key("messaging.rabbitmq.destination.routing_key")

    // Message attributes
    MessagingMessageIDKey = attribute.Key("messaging.message.id")
)

// Constant values
const MessagingSystemRabbitMQ = "rabbitmq"

// Operation types
const (
    OperationTypeSend    = "send"
    OperationTypeReceive = "receive"
    OperationTypeSettle  = "settle"
)

// Operation names
const (
    OperationNameSend    = "send"
    OperationNameReceive = "receive"
    OperationNameAck     = "ack"
    OperationNameNack    = "nack"
    OperationNameRequeue = "requeue"
)
```

### 10.10 Context Structures

Define context structures to pass semantic convention data to metric collectors:

```go
// PublishContext contains contextual information for publish metrics
type PublishContext struct {
    ServerAddress   string // Server hostname or IP (server.address)
    ServerPort      int    // Server port (server.port)
    DestinationName string // Exchange:routing_key or queue name
    RoutingKey      string // Routing key if applicable
    MessageID       string // Message ID if available
}

// ConsumeContext contains contextual information for consume metrics
type ConsumeContext struct {
    ServerAddress   string // Server hostname or IP (server.address)
    ServerPort      int    // Server port (server.port)
    DestinationName string // Queue name
    MessageID       string // Message ID if available
}
```

### 10.11 Span Creation Time Attributes

The following attributes are important for sampling decisions and should be provided at span creation time:

- `messaging.destination.name`
- `messaging.operation.name`
- `messaging.operation.type`
- `server.address`
- `server.port`

### 10.12 Stability Notes

As of the OTEL Semantic Conventions v1.39.0:
- The messaging conventions are in **Development** status
- `error.type`, `server.address`, `network.peer.address`, `network.peer.port`, and `server.port` are **Stable**
- Implementations should be prepared to migrate when conventions reach stable status

---

## Appendix A: Metric Reference Summary

### Gauge Metrics

| Metric Name | Type | Description | Increment | Decrement |
|-------------|------|-------------|-----------|-----------|
| connections | Gauge | Current open connections | openConnection() | closeConnection() |
| publishers | Gauge | Current open publishers | openPublisher() | closePublisher() |
| consumers | Gauge | Current open consumers | openConsumer() | closeConsumer() |

### Counter Metrics

| Metric Name | Type | Description | Trigger Method |
|-------------|------|-------------|----------------|
| published | Counter | Total messages published | publish() |
| published_accepted | Counter | Messages accepted by broker | publishDisposition(ACCEPTED) |
| published_rejected | Counter | Messages rejected by broker | publishDisposition(REJECTED) |
| published_released | Counter | Messages released by broker | publishDisposition(RELEASED) |
| consumed | Counter | Total messages delivered | consume() |
| consumed_accepted | Counter | Messages accepted by consumer | consumeDisposition(ACCEPTED) |
| consumed_discarded | Counter | Messages discarded by consumer | consumeDisposition(DISCARDED) |
| consumed_requeued | Counter | Messages requeued by consumer | consumeDisposition(REQUEUED) |
| written_bytes | Counter | Total bytes written to network | writtenBytes(count) |
| read_bytes | Counter | Total bytes read from network | readBytes(count) |

---

## Appendix B: Call Sequence Diagrams

### Publish Flow
```
Application → Publisher.publish(message)
                ↓
              sender.send(message)
                ↓
              metricsCollector.publish()  ← Count outgoing message
                ↓
              [wait for broker]
                ↓
              onSettlement(disposition)
                ↓
              metricsCollector.publishDisposition(disposition)  ← Count outcome
                ↓
              callback.onComplete()
```

### Consume Flow
```
Broker → nativeReceiver delivers message
           ↓
         metricsCollector.consume()  ← Count incoming message
           ↓
         decode message
           ↓
         userHandler.handle(context, message)
           ↓
         context.accept()  (or requeue/discard)
           ↓
         nativeDelivery.settle(disposition)
           ↓
         metricsCollector.consumeDisposition(disposition)  ← Count outcome
```

---

## Appendix C: Example Implementations by Language

### Python (Prometheus Client)

```python
from prometheus_client import Counter, Gauge
from typing import Protocol

class MetricsCollector(Protocol):
    def open_connection(self) -> None: ...
    def close_connection(self) -> None: ...
    # ... etc

class PrometheusMetricsCollector:
    def __init__(self, prefix: str = "rabbitmq_amqp"):
        self.connections = Gauge(
            f"{prefix}_connections",
            "Current number of open connections"
        )
        self.published = Counter(
            f"{prefix}_published_total",
            "Total published messages"
        )
        # ... initialize other metrics
    
    def open_connection(self):
        self.connections.inc()
    
    def close_connection(self):
        self.connections.dec()
    
    def publish(self):
        self.published.inc()
    
    # ... implement other methods
```

### Go (Prometheus Client)

```go
import (
    "github.com/prometheus/client_golang/prometheus"
)

type MetricsCollector interface {
    OpenConnection()
    CloseConnection()
    // ... etc
}

type PrometheusMetricsCollector struct {
    connections prometheus.Gauge
    published   prometheus.Counter
    // ... other metrics
}

func NewPrometheusMetricsCollector(prefix string) *PrometheusMetricsCollector {
    return &PrometheusMetricsCollector{
        connections: prometheus.NewGauge(prometheus.GaugeOpts{
            Name: prefix + "_connections",
            Help: "Current number of open connections",
        }),
        published: prometheus.NewCounter(prometheus.CounterOpts{
            Name: prefix + "_published_total",
            Help: "Total published messages",
        }),
        // ... initialize others
    }
}

func (c *PrometheusMetricsCollector) OpenConnection() {
    c.connections.Inc()
}

func (c *PrometheusMetricsCollector) CloseConnection() {
    c.connections.Dec()
}

func (c *PrometheusMetricsCollector) Publish() {
    c.published.Inc()
}

// ... implement other methods
```

### JavaScript/TypeScript (prom-client)

```typescript
import { Counter, Gauge, Registry } from 'prom-client';

interface MetricsCollector {
  openConnection(): void;
  closeConnection(): void;
  // ... etc
}

class PrometheusMetricsCollector implements MetricsCollector {
  private connections: Gauge;
  private published: Counter;
  // ... other metrics
  
  constructor(registry: Registry, prefix: string = 'rabbitmq_amqp') {
    this.connections = new Gauge({
      name: `${prefix}_connections`,
      help: 'Current number of open connections',
      registers: [registry]
    });
    
    this.published = new Counter({
      name: `${prefix}_published_total`,
      help: 'Total published messages',
      registers: [registry]
    });
    
    // ... initialize others
  }
  
  openConnection(): void {
    this.connections.inc();
  }
  
  closeConnection(): void {
    this.connections.dec();
  }
  
  publish(): void {
    this.published.inc();
  }
  
  // ... implement other methods
}
```

### C# (.NET Prometheus)

```csharp
using Prometheus;

public interface IMetricsCollector
{
    void OpenConnection();
    void CloseConnection();
    // ... etc
}

public class PrometheusMetricsCollector : IMetricsCollector
{
    private readonly Gauge connections;
    private readonly Counter published;
    // ... other metrics
    
    public PrometheusMetricsCollector(string prefix = "rabbitmq_amqp")
    {
        connections = Metrics.CreateGauge(
            $"{prefix}_connections",
            "Current number of open connections"
        );
        
        published = Metrics.CreateCounter(
            $"{prefix}_published_total",
            "Total published messages"
        );
        
        // ... initialize others
    }
    
    public void OpenConnection()
    {
        connections.Inc();
    }
    
    public void CloseConnection()
    {
        connections.Dec();
    }
    
    public void Publish()
    {
        published.Inc();
    }
    
    // ... implement other methods
}
```

---

## Appendix D: Glossary

- **Gauge**: A metric that can go up and down (e.g., current connections)
- **Counter**: A metric that only increases (e.g., total messages published)
- **Disposition**: The outcome/settlement state of a message (accepted/rejected/released)
- **Settlement**: The act of acknowledging/finalizing the state of a message
- **Pre-settled**: Messages that are automatically acknowledged without explicit settlement
- **Tag/Label**: A key-value pair attached to a metric for dimensionality
- **Cardinality**: The number of unique time series for a metric (affected by tags)
- **Hot path**: Code that executes very frequently (performance critical)
- **No-op**: No operation; a implementation that does nothing

---

## Document Version

- **Version**: 1.1
- **Date**: 2026-02-10
- **Based on**: RabbitMQ AMQP Java Client (version 0.10.0-SNAPSHOT)
- **OTEL Semantic Conventions**: v1.39.0 ([RabbitMQ Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/messaging/rabbitmq/))
- **Author**: Generated from Java client implementation analysis

---

## Feedback and Contributions

This document is intended to be a living guide. If you implement metrics in a client library and discover improvements or corrections, please contribute back to this document.

---

End of Metrics Implementation Guide
