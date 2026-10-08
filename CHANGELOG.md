# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.0] - 2026-10-08

### Dependencies
- **ch.admin.bit.jeap:jeap-spring-boot-parent**: 41.17.1 → 41.18.1 (minor)
- **ch.admin.bit.jeap:jeap-error-handling-service**: 25.3.1 → 25.4.0 (minor)

## [2.4.0] - 2026-10-06

### Dependencies
- **ch.admin.bit.jeap:jeap-spring-boot-parent**: 41.17.0 → 41.17.1 (patch)
- **ch.admin.bit.jeap:jeap-error-handling-service**: 25.3.0 → 25.3.1 (patch)
- **ch.admin.bit.jeap.jme:jme-spring-boot-integration-test**: 8.1.3 → 8.2.0 (minor)

## [2.3.0] - 2026-10-05

### Dependencies
- **ch.admin.bit.jeap:jeap-spring-boot-parent**: 41.14.0 → 41.17.0 (minor)

## [2.2.0] - 2026-10-02

### Dependencies
- **ch.admin.bit.jeap:jeap-error-handling-service**: 25.2.0 → 25.3.0 (minor)
- **ch.admin.bit.jeap.jme:jme-spring-boot-integration-test**: 8.1.1 → 8.1.3 (patch)

## [2.1.0] - 2026-10-01

### Dependencies
- **ch.admin.bit.jeap:jeap-spring-boot-parent**: 41.13.0 → 41.14.0 (minor)
- **ch.admin.bit.jeap:jeap-oauth-mock-server**: 11.5.0 → 11.7.0 (minor)
- **ch.admin.bit.jeap:jeap-error-handling-service**: 25.0.0 → 25.2.0 (minor)

## [2.0.0] - 2026-10-01

### Dependencies
- **ch.admin.bit.jeap:jeap-spring-boot-parent**: 40.10.1 → 41.13.0 (major)
- **ch.admin.bit.jeap:jeap-oauth-mock-server**: 10.5.0 → 11.5.0 (major)
- **ch.admin.bit.jeap:jeap-error-handling-service**: 22.6.0 → 25.0.0 (major)
- **ch.admin.bit.jeap.jme:jme-spring-boot-integration-test**: 5.12.0 → 8.1.1 (major)
- **org.springframework.modulith:spring-modulith-bom**: 2.1.0 → 2.1.1 (patch)

## [1.1.1] - 2026-09-10

### Fixed

- Publish and consume order events on `jme-order-created-modulith`, with matching message contracts,
  to isolate the example from other applications using `JmeOrderCreatedEvent`.

## [1.1.0] - 2026-09-07

### Changed

- Completed generation-safe retry and discard integration with the Error Handling Service, including
  budget-aware restart recovery and running-system coverage.
- Updated the Modulith error handling starter to `1.3.1` with explicit retry/discard consumer contracts and startup
  checks, and verified that escalations use the transactional outbox without a failure-event producer contract.
- Consolidated the complete application schema into `V1__initial_schema.sql`.
- Let the running-system JDBC assertions follow the configured Hikari schema for isolated test runs.
- Updated the jEAP Spring Boot parent to `40.10.1`, retaining the Tomcat security fixes and using its
  managed Modulith error handling starter `1.3.1`, Messaging `18.10.1` and outbox `17.27.1` without redundant overrides.
- Published thin application artifacts alongside `exec`-classified executable JARs so platform-specific
  RHOS and Nivel wrappers can reuse the applications and their migrations.
- Routed failed Modulith publications through a dedicated Kafka topic.
- Updated the local OAuth mock and bundled UI logout configuration.
- Re-enabled Maven Central publishing and Sonar checks.

## [1.0.0] - 2026-08-14

### Added

- Initial OSS version
