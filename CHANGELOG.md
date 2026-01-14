# Changelog

All notable changes to this project will be documented in this file.

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
