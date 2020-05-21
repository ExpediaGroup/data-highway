# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [6.0.20] - 2020-05-20
### Added
- Loading Bay - Added `landingTimeoutMinutes` property.

## [6.0.16] - 2020-03-24
### Added
- Metric for `HttpMessageNotReadableException`.
- Pre-create Hive table directories before creating tables. 

## [6.0.15] - 2020-01-23
### Added
- Logging `HttpMessageNotReadableException`.

## [6.0.14] - 2020-01-22
### Added
- Onramp: added timer to produce requests.
- Bumped commons-compress version due to reported security vulnerability.

## [6.0.13] - 2019-12-17
- Offramp: remove throw RuntimeException after interrupt.
- Offramp: ensure all resources are closed.

## [6.0.12] - 2019-12-06
### Changed
- Onramp: rate limit warn logging of failed CIDR authorisation.
- Offramp: log error on service error
- Offramp: throw RuntimeException after interrupt

## [6.0.11] - 2019-09-02
### Changed
- Disabled LDAP health check.

## [6.0.10] - 2019-07-16
### Changed
- Onramp: Log remote address for CIDR Authorisation.
### Fixed
- Fix Hazelcast serialisation and add backoff after failure.

## [6.0.9] - 2019-07-04
### Changed
- Updated `hotels-oss-parent` to version 4.0.1 (was 2.3.1).

## [6.0.8] - 2019-05-13
### Changed
- Weigh-bridge: Added missing `hazelcast-kubernetes` dependency.
- Weigh-bridge: Retry `BrokerRefresher` in case of exception talking to the Kafka API.

## [6.0.7] - 2019-04-01
### Changed
- Paver: validate only against non-deleted schemas when updating the partitiion path.

## [6.0.6] - 2019-02-12
### Changed
- KafkaOffsetMetrics: Fix issue with classpath.

## [6.0.5] - 2019-02-12
### Added
- Offramp: added error event type and send client error when a -ve request is made.
- Offramp: CLI.
- Paver: Allow deletion of a Hive Destination.
- Onramp: Added messages-per-request metric.
### Changed
- Switch from Redis to Hazelcast for session storage.
- KafkaOffsetMetrics: switch to micrometer from prometheus API.

## [6.0.4] - 2019-01-31
### Added
- Onramp: Messages per request metric.
- New CLI Offramp client tool
- New service exposed by weigh-bridge to collect aggregate Kafka metrics.

### Changed
- Increased highway patrol onrampTime metric window from 30 seconds to 10 minutes.
- Switch endpoint session storage to Hazelcast from Redis

## [6.0.3] - 2019-01-29
### Changed
- Switched all metrics to micrometer API with prometheus actuator (except truck park).
- Fixed tests working around the disabling of bean overriding in Spring Boot 2.1.x
- Modification of the Test Drive certificate to allow multi domains (`localhost` and `test-drive`).
- Increased highway patrol transitTime metric window from 30 seconds to 10 minutes.
- Modification of the WebSocket send implementation used in offramp client. 
- Implementation of S3 connectivity checks on startup for Loading Bay and Towtruck.

## [6.0.2] - 2019-01-23
### Changed
- Made onramp producer request timeout configurable.
- Removed swagger's jq requirement.

## [6.0.1] - 2019-01-17
### Removed
- Removed spring-boot-maven-plugin.

## [6.0.0] - 2019-01-17 [YANKED]
### Added
- First public release.
