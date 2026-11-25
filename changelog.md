# Change Log

This file contains all the notable changes done to the Ballerina Solace package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- [Fix a `solace:Listener` service in `AUTO_ACK` mode losing every guaranteed message whose `onMessage` returns an error or panics, because the message was acknowledged before `onMessage` ran; the message is now acknowledged after `onMessage` returns successfully, settled as failed and redelivered (up to the queue's maximum redelivery count) when `onMessage` returns an error or panics, and rejected when it cannot be converted or bound to the `onMessage` parameter type](https://github.com/ballerina-platform/ballerina-library/issues/9834)
- [Fix a message sent with an empty `userData` array never being accepted by the broker, which made `sendAwaitAcknowledgement` time out and blocked later guaranteed sends on the same producer; an empty `userData` is now sent as if the field were absent](https://github.com/ballerina-platform/ballerina-library/issues/9865)
- [Fix the `senderTimestamp`, `receiveTimestamp` and `sequenceNumber` of a received message being truncated to 32 bits, which made every epoch-millisecond timestamp and any sequence number above 2^31-1 wrong, for both `MessageConsumer` and `Listener`](https://github.com/ballerina-platform/ballerina-library/issues/9844)
- [Fix `secureSocket.trustedCommonNames` being ignored, so a broker certificate whose common name was not in the list was accepted; a connection now fails when the certificate's common name is not in a non-empty list](https://github.com/ballerina-platform/ballerina-library/issues/9861)

### Changed

- [Make `caller->ack()` and `caller->nack()` log a warning and do nothing in an `AUTO_ACK` service, since the listener settles the message itself after `onMessage` returns](https://github.com/ballerina-platform/ballerina-library/issues/9834)
- [Log a warning when `secureSocket.keyStore` is set, and document that the key store is not used and the `auth` configuration authenticates, since client certificate authentication is not supported yet](https://github.com/ballerina-platform/ballerina-library/issues/9862)
- [Reject a `trustedCommonNames` entry that is empty, contains a comma, or has leading or trailing whitespace, since such a name can never match a certificate's common name](https://github.com/ballerina-platform/ballerina-library/issues/9861)
