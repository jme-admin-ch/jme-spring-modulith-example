# jme-spring-modulith-example

Example project demonstrating how to build a [Spring Modulith](https://spring.io/projects/spring-modulith)
application on the jEAP platform: application modules integrated through internal asynchronous events,
an external event consumed from Kafka with
[jeap-messaging](https://github.com/jeap-admin-ch/jeap-messaging), failed message processing handed
over to the [jEAP Error Handling Service](https://github.com/jeap-admin-ch/jeap-error-handling), and a
REST API protected by the
[jEAP security starter](https://github.com/jeap-admin-ch/jeap-spring-boot-starters) with semantic roles.

The domain follows the
[spring-modulith-example-full](https://github.com/spring-projects/spring-modulith/tree/main/spring-modulith-examples/spring-modulith-example-full)
sample — an order that, once completed, is picked up by an inventory — extended with what a jEAP
service actually needs around it.

The example consists of the following modules:

* **jme-spring-modulith-scs**: The Spring Modulith service, with the application modules `order`,
  `inventory`, `notification`, `shipping` and `messaging`
* **jme-spring-modulith-auth-scs**: An instance of the
  [jEAP OAuth mock server](https://github.com/jeap-admin-ch/jeap-oauth-mock-server) used as
  authorization server
* **jme-spring-modulith-error-scs**: An instance of the jEAP Error Handling Service
* **jme-spring-modulith-test**: Automated integration tests covering the local Docker Compose setup

This repository is platform-agnostic: it contains the example services and publishes them as Maven
artifacts that platform-specific (non-public) deployments build on top of.

This README is the entry point. Deeper documentation lives in `docs/`:

| Page                                                                            | Content                                                                                                                |
|---------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| [Architecture](docs/architecture.md)                                            | The application modules, how a message travels through the system, and the two failure paths                             |
| [Configuration](docs/configuration.md)                                          | Every property, port, topic, role and client the example uses, and why                                                   |
| [Design: async event error handling](docs/async-event-error-handling-design.md) | Design for the **planned** bridge that escalates failed internal asynchronous events to the jEAP Error Handling Service |

## Changes

This project is versioned using [Semantic Versioning](http://semver.org/) and all changes are
documented in [CHANGELOG.md](./CHANGELOG.md) following the format defined in
[Keep a Changelog](http://keepachangelog.com/).

## The application modules

Spring Modulith derives the application modules from the package structure: every direct sub-package
of the application's base package is one module. A module's own types are its API; everything in a
nested package is internal and inaccessible to the other modules.

```
ch.admin.bit.jme.modulith
├── order          owns the orders; publishes OrderCompleted
├── inventory      reserves stock,        reacting to OrderCompleted
├── notification   records notifications, reacting to OrderCompleted
├── shipping       hands over to carrier, reacting to OrderCompleted — and can fail doing so
└── messaging      Kafka adapter: consumes JmeOrderCreatedEvent, calls the order module
```

`messaging` is the only module that knows the Avro types generated from the
[jme message type registry](https://github.com/jme-admin-ch/jme-message-type-registry). The domain
modules deal in their own vocabulary and would not change if the external contract did — an
anti-corruption layer expressed as a module boundary.

Each module declares which other modules it may depend on:

```java
@ApplicationModule(displayName = "Inventory", allowedDependencies = "order")
package ch.admin.bit.jme.modulith.inventory;
```

`ModularityTests` turns those declarations into a build-time constraint: it fails when a module
reaches into another module's `internal` package, depends on a module it did not declare, or when the
modules form a cycle. Without that test the package structure would be a convention; with it, it is
enforced.

At runtime, `/actuator/modulith` reports the module model of the running application.

### Generated documentation

`DocumentationTests` generates the [Spring Modulith documentation](https://docs.spring.io/spring-modulith/reference/documentation.html)
during the build, from the same module model the application runs on — so it cannot drift away from
the code the way a hand-written architecture chapter does. After `./mvnw install` (or
`./mvnw test -pl jme-spring-modulith-scs`) it can be inspected in
`jme-spring-modulith-scs/target/spring-modulith-docs`:

| File                    | Content                                                                                                                   |
|-------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `components.puml`       | C4 component diagram of all modules and their relationships                                                               |
| `module-<name>.puml`    | One diagram per module, showing it and its direct dependencies                                                            |
| `module-<name>.adoc`    | The module canvas: Spring beans, aggregate roots, published events, events listened to, configuration properties          |
| `all-docs.adoc`         | Aggregating document linking all diagrams and canvases                                                                    |

For example, the canvas of the `inventory` module records that it listens to `OrderCompleted`
asynchronously, without anybody having written that down:

```asciidoc
|Spring components
|_Services_

* `c.a.b.j.m.i.InventoryManagement`
|Events listened to
|* `c.a.b.j.m.o.OrderCompleted` (async)
```

The `.puml` files are PlantUML sources; render them with any PlantUML tooling to get images. The
documentation is a local build output and is deliberately not published as a Maven artifact.

## Internal asynchronous events

When an order is registered, the `order` module publishes an `OrderCompleted` event. The `inventory`,
`notification` and `shipping` modules pick it up:

```java
@ApplicationModuleListener
void on(OrderCompleted event) { … }
```

`@ApplicationModuleListener` is Spring Modulith's shortcut for
`@Async @Transactional(REQUIRES_NEW) @TransactionalEventListener`: the listener runs on another
thread, only after the publishing transaction has committed, and in a transaction of its own. The
three listeners are unaware of each other — they run concurrently and one failing neither rolls back
nor blocks the others.

Delivery is tracked in the **event publication registry** (`spring-modulith-starter-jdbc`). Before a
listener is invoked, a row is written to `event_publication`; it is marked `COMPLETED` when the
listener returns normally and `FAILED` when it throws. Publications left behind by an instance that
died mid-flight are republished on the next startup
(`spring.modulith.events.republish-outstanding-events-on-restart`), and the staleness monitor
(`spring.modulith.events.staleness.*`) marks publications that got stuck in `PROCESSING` as `FAILED`
so they become eligible for a retry. Because a publication can be replayed, the listeners are
idempotent.

The registry table is created by [`V1__event_publication.sql`](jme-spring-modulith-scs/src/main/resources/db/migration/V1__event_publication.sql)
rather than by Spring Modulith itself, because it holds application state that outlives a restart and
therefore deserves a migration history like any other table.

### When processing an internal event fails

The `shipping` module is the one that can fail: an order of type `FAIL_ASYNC` makes handing the
shipment over to the carrier throw. The `inventory` and `notification` listeners of the very same
event complete normally — each listener has its own row in `event_publication` and its own
transaction.

Spring Modulith persists failed publications but does not retry them on a schedule by itself: it
offers `FailedEventPublications.resubmit(ResubmissionOptions)` and leaves the retry policy to the
application. [`FailedEventPublicationResubmitter`](jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/FailedEventPublicationResubmitter.java)
is that policy:

```java
@Scheduled(fixedDelayString = "${jme.modulith.event-resubmission.interval}")
void resubmitFailedPublications() {
    failedEventPublications.resubmit(ResubmissionOptions.defaults()
            .withMinAge(minAge)
            .withFilter(this::hasRetriesLeft));
}
```

| Property                                              | Value | Meaning                                                                                        |
|-------------------------------------------------------|-------|------------------------------------------------------------------------------------------------|
| `jme.modulith.event-resubmission.interval`            | `5s`  | How often failed publications are looked at                                                     |
| `jme.modulith.event-resubmission.min-age`             | `2s`  | How old a failed publication must be before it is retried                                       |
| `jme.modulith.event-resubmission.max-completion-attempts` | `3` | Total invocations of the listener. Spring Modulith counts the initial one, so this is 1 + 2 retries |

Each resubmission increments the publication's `completion_attempts`. Once the budget is used up the
publication is left alone and stays `FAILED` in the registry. The values above are deliberately
impatient so the behaviour is observable while trying out the example.

### Planned: escalating exhausted retries to the error handling service

Retries running out is where an internal asynchronous event needs the same treatment a failed Kafka
message already gets. An error handling bridge for that is planned but **not implemented yet**:

```
Kafka event
  → internal async event
    → async event processor fails
      → Spring Modulith resubmission          ← implemented (FailedEventPublicationResubmitter)
        → retries exhausted                   ← implemented (onRetriesExhausted)
          → bridge publishes ModulithPublicationProcessingFailedEvent      ← planned
            → jEAP Error Handling Service                                  ← planned
              → RetryModulithPublicationCommand / DiscardModulithPublicationCommand
                → this application resubmits or discards the publication   ← planned
```

This example is the fixture for that feature: `FailedEventPublicationResubmitter.onRetriesExhausted(…)`
is the seam the bridge will hook into, and the `shipping` module provides a failing listener that
reliably drives a publication into that state. Today the method only reports the failure.

Two tests prove the hooks that design depends on, rather than assuming them:
`AsyncEventFailureHooksIntegrationTests` for detecting a failed publication, inspecting it without
resubmitting it, retrying a single one by identifier and discarding one without invoking the listener;
and `AsyncEventFailureNotificationIntegrationTests` for being notified of a failure proactively,
without polling. The design is written up in
[docs/async-event-error-handling-design.md](docs/async-event-error-handling-design.md).

Note that this is a **different path** from the one the `messaging` module takes. A Kafka message whose
consumption fails synchronously is escalated to the error handling service by the jEAP error handler
right away and never reaches the event publication registry — see [Error handling](#error-handling)
below.

## Security: one semantic role per application module

The REST API is an OAuth2 resource server via `jeap-spring-boot-security-starter`. Setting
`jeap.security.oauth2.resourceserver.system-name: jme` activates the **semantic** role model, in which
a role has the shape `system_%tenant_@resource_#operation` instead of being an opaque string.

Every application module owns one semantic resource, so the authorization boundaries of the service
are exactly its module boundaries:

| Endpoint                                    | `@PreAuthorize`                    | Required role              |
|---------------------------------------------|------------------------------------|----------------------------|
| `GET /api/orders`, `GET /api/orders/{id}`   | `hasRole('order', 'read')`         | `jme_@order_#read`         |
| `POST /api/orders`                          | `hasRole('order', 'write')`        | `jme_@order_#write`        |
| `POST /api/demo/orders`                     | `hasRole('order', 'write')`        | `jme_@order_#write`        |
| `GET /api/inventory`                        | `hasRole('inventory', 'read')`     | `jme_@inventory_#read`     |
| `GET /api/notifications`                    | `hasRole('notification', 'read')`  | `jme_@notification_#read`  |
| `GET /api/shipments`, `/api/shipments/attempts` | `hasRole('shipping', 'read')`  | `jme_@shipping_#read`      |

Reading and writing are separate operations of the same resource, and a token for the `order` resource
grants nothing on the `inventory` one — `OrderApiSecurityTests` asserts both.

## Prerequisites

1. **Java Development Kit (JDK)**: Version 25.
2. **Docker**: For running the required infrastructure.

**Note:** Use the provided maven wrapper to build and run the project.

## Getting started

### Infrastructure

Starts a Kafka broker, a schema registry and one PostgreSQL per service:

```shell
docker compose -f docker/docker-compose.yml up
```

The broker and the schema registry use the ports the jEAP examples conventionally use (9092 and 7781),
so stop any other running jme example first.

### Build

```shell
./mvnw install
```

### Start

Each service is started with the `local` profile, in this order:

```shell
./mvnw --projects jme-spring-modulith-auth-scs  spring-boot:run -Dspring-boot.run.profiles=local
./mvnw --projects jme-spring-modulith-error-scs spring-boot:run -Dspring-boot.run.profiles=local
./mvnw --projects jme-spring-modulith-scs       spring-boot:run -Dspring-boot.run.profiles=local
```

| Service                         | URL                                              |
|---------------------------------|--------------------------------------------------|
| `jme-spring-modulith-scs`       | http://localhost:8090/jme-spring-modulith-scs     |
| `jme-spring-modulith-auth-scs`  | http://localhost:8091/jme-spring-modulith-auth-scs |
| `jme-spring-modulith-error-scs` | http://localhost:8092/error-handling              |

## Trying it out

Every call needs an access token. The OAuth mock server issues one for the client
`jme-spring-modulith-client`, which carries one role per application module:

```shell
TOKEN=$(curl -s -X POST http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-client \
  -d client_secret=secret | jq -r .access_token)
```

### The happy path

`POST /api/demo/orders` publishes a `JmeOrderCreatedEvent` to the topic `jme-order-created`. In a real
deployment that event would come from another system; publishing it here keeps the example runnable
with nothing but curl.

```shell
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=demo-1&orderType=STANDARD"
```

The event is consumed by the `messaging` module, which registers the order in the `order` module,
which publishes `OrderCompleted`, which the two other modules pick up asynchronously:

```shell
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/jme-spring-modulith-scs/api/orders
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/jme-spring-modulith-scs/api/inventory
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/jme-spring-modulith-scs/api/notifications
curl -H "Authorization: Bearer $TOKEN" http://localhost:8090/jme-spring-modulith-scs/api/shipments
```

The event publication registry shows one completed row per listener:

```shell
docker compose -f docker/docker-compose.yml exec jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local \
  -c "select listener_id, status from event_publication;"
```

```
                                    listener_id                                    |  status
-----------------------------------------------------------------------------------+-----------
 ...inventory.InventoryManagement.on(...order.OrderCompleted)                       | COMPLETED
 ...notification.NotificationManagement.on(...order.OrderCompleted)                 | COMPLETED
```

### A failing internal asynchronous event

An order of type `FAIL_ASYNC` is consumed from Kafka without trouble and registered, but the
`shipping` listener of the resulting `OrderCompleted` event throws:

```shell
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=demo-3&orderType=FAIL_ASYNC"
```

Watch the listener being retried, and stopping after the third attempt:

```shell
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/shipments/attempts
# {"demo-3":1} … {"demo-3":2} … {"demo-3":3}  and then no further
```

The other two listeners of the same event were not affected, and the registry shows exactly that —
two `COMPLETED` rows and one `FAILED` row that has used up its attempts:

```shell
docker compose -f docker/docker-compose.yml exec jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local \
  -c "select listener_id, status, completion_attempts from event_publication;"
```

### The module model at runtime

```shell
curl -u actuator:secret http://localhost:8090/jme-spring-modulith-scs/actuator/modulith
```

The endpoint is part of `spring-modulith-starter-insight`. The actuator security chain of
`jeap-spring-boot-monitoring-starter` denies every endpoint it does not know, so it is listed under
`jeap.monitor.actuator.additional-permitted-endpoints` and enabled with
`enable-admin-endpoints: true` in the **local profile only** — jEAP requires that setting to stay off
on acceptance and production.

### Authorization

A request without a token is rejected by the resource server, and a token without the required
semantic role is rejected by method security:

```shell
# 401 — no token
curl -i http://localhost:8090/jme-spring-modulith-scs/api/orders

# 403 — authenticated, but the token carries no jme_@order_#read role
NO_ROLES=$(curl -s -X POST http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-client-without-roles \
  -d client_secret=secret | jq -r .access_token)
curl -i -H "Authorization: Bearer $NO_ROLES" http://localhost:8090/jme-spring-modulith-scs/api/orders
```

## Error handling

The Kafka consumer treats two order types as failures, so that both temporalities of the jEAP error
handling can be demonstrated without breaking anything:

| `orderType`      | Exception                             | Temporality | What the error handling service does                                                   |
|------------------|---------------------------------------|-------------|-----------------------------------------------------------------------------------------|
| `FAIL_TEMPORARY` | `TemporaryOrderProcessingException`   | `TEMPORARY` | Resends the message per the resending strategy (3 retries, 10s apart), then escalates it |
| `FAIL_PERMANENT` | `PermanentOrderProcessingException`   | `PERMANENT` | Records it and waits for a manual decision — no automatic resend                        |

Both exceptions implement `ch.admin.bit.jeap.messaging.avro.errorevent.MessageHandlerExceptionInformation`,
which is what tells the jEAP error handler how to report the failure. The whole wiring between the
consumer and the error handling service is a single property in the service's `application.yml`:

```yaml
jeap:
  messaging:
    kafka:
      errorTopicName: jme-messageprocessing-failed
```

Provoke a permanent failure and look it up by trace id — the response of the demo endpoint returns the
trace id that ties the call, the consumption and the resulting error entry together:

```shell
TRACE=$(curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=demo-2&orderType=FAIL_PERMANENT" \
  | jq -r .traceId)

# The error handling service has its own audience and roles, so it needs its own token
ERROR_TOKEN=$(curl -s -X POST http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-error-client \
  -d client_secret=secret | jq -r .access_token)

curl -s -X POST "http://localhost:8092/error-handling/api/error/?pageIndex=0&pageSize=20" \
  -H "Authorization: Bearer $ERROR_TOKEN" -H 'Content-Type: application/json' \
  -d "{\"dateFrom\":\"\",\"dateTo\":\"\",\"eventName\":\"\",\"traceId\":\"$TRACE\",\"eventId\":\"\",\"stacktracePattern\":\"\",\"states\":null,\"sortField\":\"created\",\"sortOrder\":\"desc\",\"closingReason\":\"\"}" | jq
```

Replacing `FAIL_PERMANENT` with `FAIL_TEMPORARY` shows the other path: the same error appears, but the
error handling service keeps resending the message every 10 seconds until the retries are exhausted.

This example ships no UI. The error handling service is configured with the context path
`/error-handling` that the jEAP error handling UI expects, so a locally running instance of that UI can
be pointed at it.

## Tests

### Module tests

`./mvnw test -pl jme-spring-modulith-scs` runs, against a PostgreSQL started by Testcontainers:

* `ModularityTests` — verifies the module arrangement
* `DocumentationTests` — generates the module documentation described above
* `OrderIntegrationTests`, `InventoryIntegrationTests` — `@ApplicationModuleTest` slices that bootstrap
  a single module. A hidden dependency on another module shows up here as a missing bean rather than
  passing unnoticed.
* `OrderApiSecurityTests` — drives the REST API through MockMvc with tokens built by
  `JeapAuthenticationTestTokenBuilder`
* `AsyncEventFailureHooksIntegrationTests` — the Spring Modulith hooks for detecting, retrying and
  discarding a failed event publication, which the planned error handling bridge will build on
* `AsyncEventFailureNotificationIntegrationTests` — the proactive hook that reports a failed
  publication without polling, and what it cannot see

### Integration tests

`jme-spring-modulith-test` starts the infrastructure through Spring Boot's Docker Compose support and
the three services as Maven subprocesses, then exercises the running system:

* `SpringModulithExampleIT` — the happy path from the Kafka event through to all three listeners, and
  the semantic role authorization
* `ErrorHandlingIT` — the two Kafka consumption failures escalated to the error handling service
* `InternalAsyncEventRetryIT` — the failing internal asynchronous event, its retries and their
  exhaustion

The tests are named `*IT` and therefore run in the `verify` phase:

```shell
# Build and install all local modules
./mvnw install -pl '!:jme-spring-modulith-test'
# Run the integration tests
./mvnw verify -pl jme-spring-modulith-test
```

Stop any manually started service first — the tests bind the same ports.

On CI the `CI` environment variable must be set. This activates the `ci` Spring profile in addition to
`local`, which uses `docker-compose-ci.yml` as an overlay and addresses the infrastructure by container
name, so that builds can run in parallel on an isolated Docker network.

## Note

This repository is part of the open source distribution of JME. See
[github.com/jme-admin-ch/jme](https://github.com/jme-admin-ch/jme) for more information.

## License

This repository is Open Source Software licensed under the [Apache License 2.0](./LICENSE).
