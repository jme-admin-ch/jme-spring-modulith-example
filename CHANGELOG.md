# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-09-04

### Changed

- Completed generation-safe retry and discard integration with the Error Handling Service, including
  budget-aware restart recovery and running-system coverage.
- Updated the Modulith error handling starter to `1.2.1` and verified that escalations are persisted
  and relayed through the transactional outbox.
- Updated the jEAP Spring Boot parent to `40.10.0`, including the latest Tomcat security fixes.
- Published thin application artifacts alongside `exec`-classified executable JARs so platform-specific
  RHOS and Nivel wrappers can reuse the applications and their migrations.
- Routed failed Modulith publications through a dedicated Kafka topic.
- Updated the local OAuth mock and bundled UI logout configuration.
- Re-enabled Maven Central publishing and Sonar checks.

## [1.0.0] - 2026-08-14

### Added

- Initial OSS version
