# Configuration

Everything the example is wired with, and why. See [Architecture](architecture.md) for what these
settings add up to.

## Ports

The Kafka broker and the schema registry use the ports the jEAP examples conventionally use, so only
one jme example can run at a time. The services themselves use a range of their own.

| Port   | What                                                       |
|--------|------------------------------------------------------------|
| `8090` | `jme-spring-modulith-scs`, context path `/jme-spring-modulith-scs` |
| `8091` | `jme-spring-modulith-auth-scs`, context path `/jme-spring-modulith-auth-scs` |
| `8092` | `jme-spring-modulith-error-scs`, context path `/error-handling` |
| `9092` | Kafka broker (SASL_PLAINTEXT, SCRAM-SHA-512, `user`/`user-secret`) |
| `7781` | Schema registry                                            |
| `5540` | PostgreSQL of the modulith service (`modulith`/`secret`)   |
| `5541` | PostgreSQL of the error handling service (`errorhandling`/`secret`) |

The Error Handling Service dependency includes its UI under `/error-handling`. The OAuth mock allows
its login and post-logout redirects, including the exact local logout target
`http://localhost:8092/error-handling/`.

## Topics

| Topic                             | Message                                            | Produced by                          | Consumed by                    |
|-----------------------------------|----------------------------------------------------|--------------------------------------|--------------------------------|
| `jme-order-created`               | `JmeOrderCreatedEvent`                             | the demo endpoint (stands in for an external system) | `messaging` module   |
| `jme-messageprocessing-failed`    | `MessageProcessingFailedEvent`                     | the jEAP messaging error handler     | `jme-spring-modulith-error-scs` |
| `jme-modulith-publication-processing-failed` | `ModulithPublicationProcessingFailedEvent` | the Modulith error handling starter | `jme-spring-modulith-error-scs` |
| `jme-retry-modulith-publication` | `RetryModulithPublicationCommand` | `jme-spring-modulith-error-scs` | the Modulith error handling starter |
| `jme-discard-modulith-publication` | `DiscardModulithPublicationCommand` | `jme-spring-modulith-error-scs` | the Modulith error handling starter |
| `jme-messageprocessing-deadletter`| `MessageProcessingFailedEvent`                     | the error handling service           | nobody (monitored)             |

`JmeOrderCreatedEvent` comes from the
[jme message type registry](https://github.com/jme-admin-ch/jme-message-type-registry) as a released
artifact (`ch.admin.bit.jeap.jme.messagetype.jme:jme-order-created-event`); its topic name is a
constant on the generated `TypeRef`, so it is never spelled out in the code.

## Spring Modulith

`jme-spring-modulith-scs/src/main/resources/application.yml`:

| Property                                                        | Value    | Why                                                                                                     |
|-----------------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------|
| `spring.modulith.events.jdbc.schema-initialization.enabled`     | `false`  | The `event_publication` table is owned by Flyway (`V1__initial_schema.sql`), like every other table    |
| `spring.modulith.events.completion-mode`                        | `update` | Keep publications after completion instead of deleting or archiving them, so failures stay inspectable    |
| `spring.modulith.events.republish-outstanding-events-on-restart`| `false`  | Prevent startup from replaying incomplete publications outside the starter's retry budget                 |
| `spring.modulith.events.staleness.check-intervall`              | `30s`    | How often to look for publications that got stuck                                                         |
| `spring.modulith.events.staleness.published`                    | `2m`     | Mark an old never-started publication failed so the starter applies its retry policy                      |
| `spring.modulith.events.staleness.processing`                   | `2m`     | After this, a publication stuck in `PROCESSING` is marked `FAILED` and becomes retryable again             |
| `spring.modulith.events.staleness.resubmitted`                  | `2m`     | The same for a publication stuck in `RESUBMITTED`                                                          |

All incomplete states recover by first becoming `FAILED`. The starter's generation-aware database
claim then remains the only path that increments attempts and enforces `max-completion-attempts`.

The staleness monitor and the starter's retry and reconciliation loops are scheduled tasks, so
`Application` is annotated `@EnableScheduling`.

## Failed internal asynchronous events

The `jeap-spring-modulith-error-handling-starter` owns the persistent retry and escalation policy for
failed JDBC v2 publications. Its
[configuration guide](https://github.com/jeap-admin-ch/jeap-spring-modulith-error-handling-starter/blob/main/docs/getting-started.md#configuration)
documents all properties and their defaults. This example only overrides the following values:

| Property                                               | Value                                            |
|--------------------------------------------------------|--------------------------------------------------|
| `retry-interval`                                       | `5s`                                             |
| `retry-min-age`                                        | `2s`                                             |
| `max-completion-attempts`                              | `3`                                              |
| `reconciliation-interval`                              | `5s`                                             |
| `reconciliation-min-age`                               | `2s`                                             |
| `failure-event-topic`                                  | `jme-modulith-publication-processing-failed`     |
| `retry-command-topic`                                  | `jme-retry-modulith-publication`                 |
| `discard-command-topic`                                | `jme-discard-modulith-publication`               |

The values are deliberately impatient so the behaviour is observable while trying out the example. A
real service would use longer intervals. Module tests set both initial delays to one hour so scheduled
sweeps cannot race tests that invoke the policy directly. `completion_attempts` keeps the retry budget across restarts.
Once the budget is exhausted, the starter publishes a failure event through the transactional outbox
and records `(publication_id, completion_attempts)` in `modulith_publication_failure`. The Error
Handling Service stores the event with origin `MODULITH_PUBLICATION` and publishes a UUID-exact retry
or discard command on the consumed Kafka cluster when an operator acts on it.

## Error handling service

`jme-spring-modulith-error-scs`:

| Property                                                              | Value                              | Meaning                                                        |
|-----------------------------------------------------------------------|------------------------------------|------------------------------------------------------------------|
| `jeap.errorhandling.topic`                                            | `jme-messageprocessing-failed`     | Where it looks for failures                                      |
| `jeap.errorhandling.modulithPublicationProcessingFailedTopic`         | `jme-modulith-publication-processing-failed` | Where it looks for failed Modulith publications                  |
| `jeap.errorhandling.deadLetterTopicName`                              | `jme-messageprocessing-deadletter` | Where its own unprocessable messages go                          |
| `jeap.errorhandling.resend.default-resending-strategy.max-retries`    | `3`                                | Resends of a temporary failure before it becomes permanent       |
| `jeap.errorhandling.resend.default-resending-strategy.delay`          | `10s`                              | Between resends                                                  |
| `jeap.errorhandling.task-management.service.enabled`                  | `false`                            | No Agir integration in this example                              |
| `jeap.errorhandling.frontend.*`                                       | set                                | Configuration served to the bundled UI, including the local logout redirect |

On the consumer side the entire wiring to the error handling service is a single property in
`jme-spring-modulith-scs`:

```yaml
jeap:
  messaging:
    kafka:
      errorTopicName: jme-messageprocessing-failed
```

## Roles and clients

`jeap.security.oauth2.resourceserver.system-name: jme` activates the semantic role model, in which a
role reads `system_%tenant_@resource_#operation`. Each domain-facing module owns one resource. The
`messaging` adapter reuses `order/write` for its demo producer because that endpoint initiates order
registration.

| Endpoint                                                                             | `@PreAuthorize`                   | Role                      |
|--------------------------------------------------------------------------------------|-----------------------------------|---------------------------|
| `GET /api/orders`, `GET /api/orders/{orderId}`                                       | `hasRole('order', 'read')`        | `jme_@order_#read`        |
| `POST /api/orders`                                                                   | `hasRole('order', 'write')`       | `jme_@order_#write`       |
| `POST /api/demo/orders`                                                              | `hasRole('order', 'write')`       | `jme_@order_#write`       |
| `GET /api/inventory`                                                                 | `hasRole('inventory', 'read')`    | `jme_@inventory_#read`    |
| `GET /api/notifications`                                                             | `hasRole('notification', 'read')` | `jme_@notification_#read` |
| `GET /api/shipments`, `/api/shipments/attempts`, `/api/shipments/publications/{id}`  | `hasRole('shipping', 'read')`     | `jme_@shipping_#read`     |

The OAuth mock server defines one browser client and four system clients. The system clients use the
client-credentials grant with the secret `secret`:

| Client                                    | Roles                                             | Audience                        | Used by                                    |
|-------------------------------------------|---------------------------------------------------|---------------------------------|--------------------------------------------|
| `error-handling-ui`                       | EHS view, retry, delete and error-group roles      | `jme-spring-modulith-error-scs` | EHS UI authorization-code flow             |
| `jme-spring-modulith-client`              | roles for order, inventory, notification and shipping resources | `jme-spring-modulith-scs` | the local walkthrough, the tests           |
| `jme-spring-modulith-client-without-roles`| `jme_@unrelated_#read`                            | `jme-spring-modulith-scs`       | demonstrating the 403 path                 |
| `jme-spring-modulith-error-client`        | `jme_@error_#view/#retry/#delete`, `jme_@errorgroup_#view/#edit` | `jme-spring-modulith-error-scs` | querying the error handling service        |
| `jme-spring-modulith-error-service`       | the same error roles                              | `jme-spring-modulith-error-scs` | the error handling service's own outgoing calls |

The audience matters: each service validates that its own application name is in the `aud` claim, so a
token for the modulith service is rejected by the error handling service and vice versa. That is why
querying the errors needs a second token.

## Actuator

`jeap-spring-boot-monitoring-starter` disables actuator endpoints by default, enables a known set, and
denies anything else. The Spring Modulith endpoint is not in that set, so it needs two settings:

```yaml
jeap:
  monitor:
    actuator:
      additional-permitted-endpoints:
        - org.springframework.modulith.actuator.ApplicationModulesEndpoint
```

```yaml
# application-local.yml only
jeap:
  monitor:
    actuator:
      enable-admin-endpoints: true
```

`additional-permitted-endpoints` has no effect unless `enable-admin-endpoints` is true, and jEAP
requires that to stay false on acceptance and production — hence the split between the two files.
`/actuator/modulith` is then reachable with the `actuator` basic-auth user.

## Profiles

| Profile | Where it comes from                    | What it changes                                                        |
|---------|----------------------------------------|--------------------------------------------------------------------------|
| `local` | passed on the command line             | Ports, credentials, the authorization server issuer, Kafka on `localhost` |
| `ci`    | activated when `CI` is set, next to `local` | Kafka and the databases addressed by container name instead of `localhost` |
| `test`  | the module tests                       | Kafka listeners off, a dummy issuer, no schema registry                   |

The integration tests resolve `local` or `local,ci` through
`BootServiceIntegrationTestBase.TestProfileResolver`, so the CI profile is only ever an overlay.

## Testcontainers

`jme-spring-modulith-scs/src/test/resources/testcontainers.properties` sets `hub.image.name.prefix`, so
the PostgreSQL image and the Ryuk sidecar Testcontainers starts itself are both pulled from the
configured registry. Image names in the test code stay plain.

## Related

- [Architecture](architecture.md)
- [Local walkthrough](local-walkthrough.md)
- [Async event error handling](async-event-error-handling-design.md)
