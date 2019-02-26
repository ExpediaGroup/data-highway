# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

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

## [6.0.4]
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
