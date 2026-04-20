# Changelog

All notable changes to this project will be documented in this file.

## [[1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v1.0.0)]

## 1.0.0 - 2026-04-20
- [Release 1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v1.0.0)
- First stable release.

### Added
- Add `release.yml` for changelog generation by @Gsantomaggio in [#89](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/89)
- Add support for JMS queue by @Gsantomaggio in [#91](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/91)
- Add `DelayedQueueSpecification` for Tanzu delayed queues by @Gsantomaggio in [#92](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/92)
- Add `PublishAsync` to Publisher for non-blocking message publishing by @Gsantomaggio in [#93](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/93)

### Changed
- Bump `go.opentelemetry.io/otel/sdk` from 1.40.0 to 1.43.0 in `/tests/otelmetrics` by @dependabot in [#94](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/94)

## [[0.7.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.7.0)]

## 0.7.0 - 2026-03-17
- [Release 0.7.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.7.0)

### Added
- Add `docs/examples/jms_queue` example for `JmsQueueSpecification`
- Add `Jms` queue type and `JmsQueueSpecification` for JMS queues
- Add code documentation by @Gsantomaggio in [#86](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/86)
- Add OpenTelemetry metrics support with semantic conventions by @Zerpet in [#84](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/84)
- Add settings to the stream queues by @Gsantomaggio in [#87](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/87)

## [[0.6.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.6.0)]

## 0.6.0 - 2026-02-16
- [Release 0.6.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.6.0)

### Added
- Add pre-settled option to consumer by @Gsantomaggio in [#80](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/80)
- Add default queue implementation by @Gsantomaggio in [#85](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/85)

### Changed
- Change consumer options by @Gsantomaggio in [#81](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/81)
- Rename Consumer Feature to Consumer SettleStrategy by @Gsantomaggio in [#82](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/82)

### Breaking changes
- Minor breaking change in [#82](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/82): Rename Consumer Feature to Consumer SettleStrategy. Unify all the AMQP 1.0 clients' interfaces.


## [[0.5.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.5.0)]

## 0.5.0 - 2026-01-22
- [Release 0.5.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.5.0)

### Breaking changes
- **RequesterOptions**: Replaced `DirectReplyTo bool` with `SettleStrategy ConsumerSettleStrategy`. Use `SettleStrategy: rabbitmqamqp.DirectReplyTo` for direct-reply-to, or leave zero value for default (dedicated reply queue). Aligns consumer/requester configuration with other AMQP 1.0 clients.
- **ConsumerOptions / amqp_types**: Renamed `ConsumerFeature` to `ConsumerSettleStrategy`, `DefaultSettle` to `ExplicitSettle`, and `Feature` field to `SettleStrategy`. Aligns with the unified settle strategy API used in other AMQP 1.0 clients.

### Added
- Add WebSocket transport support for AMQP 1.0 connections by @vedanthnyk25 in [#78](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/78)
- Add Sec-WebSocket-Protocol to the HTTP header by @Gsantomaggio in [#79](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/79)

## [[0.4.1](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.4.1)]

## 0.4.1 - 2026-01-14
- [Release 0.4.1](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.4.1)

### Changed
- Update azure to 1.5.1 by @Gsantomaggio in [#75](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/75)
  - Note: Azure 1.5.1 contains this fix [Azure/go-amqp#372](https://github.com/Azure/go-amqp/pull/372) needed for [#76](https://github.com/rabbitmq/rabbitmq-amqp-go-client/issues/76)

## [[0.4.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.4.0)]

## 0.4.0 - 2025-18-11
- [Release 0.4.0](https://github.com/rabbitmq/rabbitmq-amqp-go-client/releases/tag/v0.4.0)

### Added
- Implement direct reply to feature by @Gsantomaggio in [#73](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/73)

### Fixed
- Fix log lines with badkey by @Zerpet in [#71](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/71)

### Breaking changes
- Rename RpcClient to Requester and RpcServer to Responder by @Gsantomaggio in [#72](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/72)
