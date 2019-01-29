# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased]
### Changed
- Switched all metrics to micrometer API with prometheus actuator (except truck park).
- Fixed tests working around the disabling of bean overriding in Spring Boot 2.1.x

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
