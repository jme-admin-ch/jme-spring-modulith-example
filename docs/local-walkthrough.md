# Local walkthrough

This walkthrough starts the complete example, exercises its successful and failing paths, and shows
where to inspect the resulting state. Run all commands from the repository root in a Bash shell.

## What the example demonstrates

The example has three runnable services:

| Service | Purpose | Local URL |
|---|---|---|
| `jme-spring-modulith-scs` | Spring Modulith application | `http://localhost:8090/jme-spring-modulith-scs` |
| `jme-spring-modulith-auth-scs` | OAuth mock server | `http://localhost:8091/jme-spring-modulith-auth-scs` |
| `jme-spring-modulith-error-scs` | jEAP Error Handling Service (EHS) and UI | `http://localhost:8092/error-handling` |

The main flow is:

```text
JmeOrderCreatedEvent on Kafka
  -> messaging module
    -> order module persists the order and publishes OrderCompleted
      -> inventory, notification and shipping process OrderCompleted asynchronously
```

Two independent failure paths are demonstrated:

| Failure | Trigger | Handling |
|---|---|---|
| Kafka message processing | `FAIL_TEMPORARY` or `FAIL_PERMANENT` | jEAP messaging reports the failed Kafka message to EHS |
| Internal asynchronous publication | `FAIL_ASYNC` | The Modulith starter retries the shipping publication and escalates it to EHS when exhausted |

## 1. Prerequisites

You need:

- JDK 25
- Docker with Docker Compose
- Bash, `curl` and `jq`
- Access to the Maven repositories used by the project
- Access to the container images under `repo.bit.admin.ch:8444`
- Free ports `8090`, `8091`, `8092`, `9092`, `7781`, `5540` and `5541`

Change to the repository root and ensure the CI-only profile is not activated:

```bash
cd jme-spring-modulith-example
unset CI
```

## 2. Build the applications

Build and install the runnable modules without starting the end-to-end test suite:

```bash
./mvnw install -pl '!:jme-spring-modulith-test'
```

The excluded module starts its own Compose stack and binds the same ports. Run it separately after
stopping the manually started services, as described under [Run the tests](#11-run-the-tests).

## 3. Start the infrastructure

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

The stack contains:

| Component | Address or credentials |
|---|---|
| Kafka | `localhost:9092`, SASL_PLAINTEXT, SCRAM-SHA-512, `user` / `user-secret` |
| Schema Registry | `http://localhost:7781` |
| Modulith PostgreSQL | `localhost:5540`, database/user `jme-spring-modulith-db-local` / `modulith`, password `secret` |
| EHS PostgreSQL | `localhost:5541`, database `jeap-error-handling-service-db-local`, user `errorhandling`, password `secret` |

If Kafka initialization raced broker startup, restart its one-shot initializer:

```bash
docker compose -f docker/docker-compose.yml restart broker_init
```

## 4. Start the services

Run each command in a separate terminal, in this order.

Terminal 1, OAuth mock server:

```bash
./mvnw --projects jme-spring-modulith-auth-scs \
  spring-boot:run \
  -Dspring-boot.run.profiles=local
```

Terminal 2, Error Handling Service:

```bash
./mvnw --projects jme-spring-modulith-error-scs \
  spring-boot:run \
  -Dspring-boot.run.profiles=local
```

Terminal 3, Spring Modulith application:

```bash
./mvnw --projects jme-spring-modulith-scs \
  spring-boot:run \
  -Dspring-boot.run.profiles=local
```

Wait until all three applications report that they have started.

## 5. Obtain access tokens

Run the remaining commands in a fourth terminal. Keep this terminal open so the shell variables remain
available.

Token for the application API:

```bash
TOKEN=$(curl -sS -X POST \
  http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-client \
  -d client_secret=secret | jq -r .access_token)
```

Token for the EHS API:

```bash
ERROR_TOKEN=$(curl -sS -X POST \
  http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-error-client \
  -d client_secret=secret | jq -r .access_token)
```

Token with the correct audience but no applicable role:

```bash
NO_ROLES=$(curl -sS -X POST \
  http://localhost:8091/jme-spring-modulith-auth-scs/oauth2/token \
  -d grant_type=client_credentials \
  -d client_id=jme-spring-modulith-client-without-roles \
  -d client_secret=secret | jq -r .access_token)
```

Verify that token acquisition succeeded:

```bash
test -n "$TOKEN" && test "$TOKEN" != "null"
test -n "$ERROR_TOKEN" && test "$ERROR_TOKEN" != "null"
```

## 6. Exercise the happy path

Use a unique order ID. Registering the same ID again is deliberately idempotent and does not publish a
second internal event.

```bash
ORDER_ID="standard-$(date +%s)"

curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=$ORDER_ID&orderType=STANDARD" | jq
```

The endpoint returns HTTP 202 with the Kafka event ID, topic, order data and trace ID. Kafka consumption
and the three internal listeners are asynchronous, so repeat the following calls briefly if the new
order is not immediately visible:

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/orders | jq

curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/inventory | jq

curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/notifications | jq

curl -sS -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/shipments | jq
```

Inspect the three durable Spring Modulith publications created for this order:

```bash
docker compose -f docker/docker-compose.yml exec -T \
  jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local \
  -c "select listener_id, status, completion_attempts
        from event_publication
       where serialized_event like '%${ORDER_ID}%'
       order by listener_id;"
```

Inventory, notification and shipping should each have one `COMPLETED` row with one completion attempt.

## 7. Verify authorization

A request without a token is rejected with HTTP 401:

```bash
curl -i http://localhost:8090/jme-spring-modulith-scs/api/orders
```

A valid token without `jme_@order_#read` is rejected with HTTP 403:

```bash
curl -i \
  -H "Authorization: Bearer $NO_ROLES" \
  http://localhost:8090/jme-spring-modulith-scs/api/orders
```

The application token succeeds with HTTP 200:

```bash
curl -i \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8090/jme-spring-modulith-scs/api/orders
```

The `messaging` module is an adapter and has no separate semantic resource. Its demo producer requires
the `order/write` role because it initiates order registration.

## 8. Exercise Kafka message failures

### Permanent failure

`FAIL_PERMANENT` makes the Kafka listener throw before it reaches the order module:

```bash
PERMANENT_ORDER_ID="permanent-$(date +%s)"

TRACE_ID=$(curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=$PERMANENT_ORDER_ID&orderType=FAIL_PERMANENT" \
  | jq -r .traceId)
```

Poll EHS by trace ID until the error appears:

```bash
for attempt in {1..30}; do
  KAFKA_ERROR=$(curl -sS -X POST \
    "http://localhost:8092/error-handling/api/error/?pageIndex=0&pageSize=20" \
    -H "Authorization: Bearer $ERROR_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"dateFrom\":\"\",\"dateTo\":\"\",\"eventName\":\"\",\"traceId\":\"$TRACE_ID\",\"eventId\":\"\",\"stacktracePattern\":\"\",\"states\":null,\"sortField\":\"created\",\"sortOrder\":\"desc\",\"closingReason\":\"\"}" \
    | jq '.errors[0] // empty')
  if [[ -n "$KAFKA_ERROR" ]]; then
    break
  fi
  sleep 2
done

test -n "$KAFKA_ERROR" || { printf 'No EHS error found for trace %s\n' "$TRACE_ID" >&2; false; }
jq . <<<"$KAFKA_ERROR"
```

The result has origin `KAFKA_MESSAGE` and state `PERMANENT`. The order does not exist because the
consumer failed before calling `OrderManagement`.

### Temporary failure

`FAIL_TEMPORARY` follows the same reporting path, but EHS automatically resends it three times with a
ten-second delay before it becomes permanent:

```bash
TEMPORARY_ORDER_ID="temporary-$(date +%s)"

curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=$TEMPORARY_ORDER_ID&orderType=FAIL_TEMPORARY" | jq
```

The EHS UI shows the temporary generations and their resulting states.

## 9. Exercise an internal asynchronous failure

### Trigger and observe automatic retries

`FAIL_ASYNC` is consumed successfully from Kafka. The order, inventory reservation and notification are
created, but the shipping listener throws:

```bash
ASYNC_ORDER_ID="async-$(date +%s)"

curl -sS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/demo/orders?orderId=$ASYNC_ORDER_ID&orderType=FAIL_ASYNC" | jq
```

Watch the listener attempts:

```bash
for attempt in {1..30}; do
  ATTEMPTS=$(curl -sS \
    -H "Authorization: Bearer $TOKEN" \
    http://localhost:8090/jme-spring-modulith-scs/api/shipments/attempts \
    | jq -r --arg id "$ASYNC_ORDER_ID" '.[$id] // 0')
  printf 'shipping attempts: %s\n' "$ATTEMPTS"
  if [[ "$ATTEMPTS" -ge 3 ]]; then
    break
  fi
  sleep 2
done
```

The initial listener invocation counts as attempt one. The configured maximum of three therefore means
one initial attempt and two automatic retries. The `/attempts` endpoint is only an in-memory
visualization aid and resets when the application restarts.

### Obtain the durable publication ID

```bash
PUBLICATION_ID=$(docker compose -f docker/docker-compose.yml exec -T \
  jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local -At \
  -c "select id
        from event_publication
       where listener_id like '%ShippingManagement.on%'
         and serialized_event like '%${ASYNC_ORDER_ID}%'
       order by publication_date desc
       limit 1;")

printf 'Publication ID: %s\n' "$PUBLICATION_ID"
test -n "$PUBLICATION_ID" || { printf 'No shipping publication found for %s\n' "$ASYNC_ORDER_ID" >&2; false; }
```

Inspect the durable state through the application API:

```bash
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/shipments/publications/$PUBLICATION_ID" | jq
```

After automatic retry exhaustion, the expected state is `FAILED` with `completionAttempts` equal to
three.

### Obtain the EHS error ID

The EHS overview represents a Modulith publication UUID as `eventId`. The separate `publicationId`
field is added only by the error-details endpoint. Match `eventId` in the list response:

```bash
ERROR_ID=""

for attempt in {1..30}; do
  EHS_JSON=$(curl -sS -X POST \
    "http://localhost:8092/error-handling/api/error/?pageIndex=0&pageSize=100" \
    -H "Authorization: Bearer $ERROR_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"dateFrom":"","dateTo":"","eventName":"","traceId":"","eventId":"","stacktracePattern":"","states":null,"sortField":"created","sortOrder":"desc","closingReason":""}')

  ERROR_ID=$(jq -r --arg publication "$PUBLICATION_ID" \
    'first(
       .errors[] |
       select(
         .origin == "MODULITH_PUBLICATION" and
         .eventId == $publication
       ) |
       .id
     ) // empty' <<<"$EHS_JSON")

  if [[ -n "$ERROR_ID" ]]; then
    break
  fi
  sleep 2
done

printf 'Error ID: %s\n' "$ERROR_ID"
test -n "$ERROR_ID" || { printf 'No EHS error found for publication %s\n' "$PUBLICATION_ID" >&2; false; }
```

If the ID is still empty, use the diagnostics under
[The internal publication exists but no EHS ID is found](#the-internal-publication-exists-but-no-ehs-id-is-found).

Inspect the fields that are available only in the details response:

```bash
curl -sS \
  -H "Authorization: Bearer $ERROR_TOKEN" \
  "http://localhost:8092/error-handling/api/error/$ERROR_ID/details" | jq
```

Inspect the best-effort serialized `OrderCompleted` payload:

```bash
curl -sS \
  -H "Authorization: Bearer $ERROR_TOKEN" \
  "http://localhost:8092/error-handling/api/error/$ERROR_ID/event/payload" | jq
```

### Retry the publication

Preserve the first generation's ID and request a retry:

```bash
FIRST_ERROR_ID="$ERROR_ID"

curl -i -X POST \
  -H "Authorization: Bearer $ERROR_TOKEN" \
  "http://localhost:8092/error-handling/api/error/$FIRST_ERROR_ID/event/retry"
```

The shipping listener runs once more. Because `FAIL_ASYNC` always fails, the publication returns to
`FAILED`, `completionAttempts` becomes four, the first EHS generation closes, and a second generation
is created.

Find the second generation:

```bash
SECOND_ERROR_ID=""

for attempt in {1..30}; do
  EHS_JSON=$(curl -sS -X POST \
    "http://localhost:8092/error-handling/api/error/?pageIndex=0&pageSize=100" \
    -H "Authorization: Bearer $ERROR_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{"dateFrom":"","dateTo":"","eventName":"","traceId":"","eventId":"","stacktracePattern":"","states":null,"sortField":"created","sortOrder":"desc","closingReason":""}')

  SECOND_ERROR_ID=$(jq -r \
    --arg publication "$PUBLICATION_ID" \
    --arg first "$FIRST_ERROR_ID" \
    'first(
       .errors[] |
       select(
         .origin == "MODULITH_PUBLICATION" and
         .eventId == $publication and
         .id != $first
       ) |
       .id
     ) // empty' <<<"$EHS_JSON")

  if [[ -n "$SECOND_ERROR_ID" ]]; then
    break
  fi
  sleep 2
done

printf 'Second error ID: %s\n' "$SECOND_ERROR_ID"
test -n "$SECOND_ERROR_ID" || { printf 'No second EHS generation found for %s\n' "$PUBLICATION_ID" >&2; false; }
```

### Discard the second generation

```bash
curl -i -X DELETE \
  -H "Authorization: Bearer $ERROR_TOKEN" \
  "http://localhost:8092/error-handling/api/error/$SECOND_ERROR_ID?reason=discarded-for-demo"
```

Discard does not invoke shipping again. It marks the publication `COMPLETED` so no scheduler selects it
again:

```bash
curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/shipments/publications/$PUBLICATION_ID" | jq
```

The completion-attempt count remains four and no shipment is created.

### Use the EHS UI

The same details, payload, retry and discard operations are available at:

```text
http://localhost:8092/error-handling
```

The OAuth mock offers the configured `user` identity for browser login.

## 10. Inspect architecture and persistence

### Runtime module model

```bash
curl -sS -u actuator:secret \
  http://localhost:8090/jme-spring-modulith-scs/actuator/modulith | jq
```

### Generated module documentation

```bash
./mvnw test -pl jme-spring-modulith-scs
```

Inspect `jme-spring-modulith-scs/target/spring-modulith-docs`.

### Publication registry

```bash
docker compose -f docker/docker-compose.yml exec -T \
  jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local \
  -c "select id, listener_id, event_type, status, completion_attempts,
             publication_date, completion_date, last_resubmission_date
        from event_publication
       order by publication_date desc;"
```

### Failure generations and application outbox

```bash
docker compose -f docker/docker-compose.yml exec -T \
  jme-spring-modulith-db-local \
  psql -U modulith -d jme-spring-modulith-db-local \
  -c "select * from modulith_publication_failure order by escalated_at desc;
      select id, topic, message_id, message_idempotence_id,
             message_type_name, sent_immediately, sent_scheduled
        from deferred_message order by id desc;"
```

### EHS errors

```bash
docker compose -f docker/docker-compose.yml exec -T \
  jeap-error-handling-service-db-local \
  psql -U errorhandling -d jeap-error-handling-service-db-local \
  -c "select e.id, e.state, ce.origin, ce.modulith_publication_id
        from data.error e
        join data.causing_event ce on ce.id = e.causing_event_id
       order by e.created desc;"
```

## 11. Run the tests

Stop the three Maven processes and the manually started infrastructure first because the integration
tests bind the same ports:

```bash
docker compose -f docker/docker-compose.yml down
```

Run the application module tests:

```bash
./mvnw test -pl jme-spring-modulith-scs
```

Run the complete end-to-end suite:

```bash
./mvnw install -pl '!:jme-spring-modulith-test'
./mvnw verify -pl jme-spring-modulith-test
```

The suite covers the happy path, authorization, both Kafka failure temporalities, automatic internal
publication retries, EHS projection and payload, generation-safe manual retry, duplicate command
handling and discard.

## 12. Stop or reset the example

Stop the three Maven processes with Ctrl-C, then stop the infrastructure while preserving its container
state:

```bash
docker compose -f docker/docker-compose.yml stop
```

Remove the containers and all local state before a clean rerun. The databases and Kafka broker do not
use persistent volumes in this example, so `down` resets their data:

```bash
docker compose -f docker/docker-compose.yml down -v --remove-orphans
```

Always use new order IDs unless deliberately testing idempotence.

## Troubleshooting

### A service does not start

Check whether another example owns one of the fixed ports:

```bash
docker compose -f docker/docker-compose.yml ps
```

Check the Compose logs:

```bash
docker compose -f docker/docker-compose.yml logs
```

If Maven uses container hostnames such as `broker` during a manual run, ensure `CI` is unset.

### The happy-path GET responses are empty

Kafka consumption and `@ApplicationModuleListener` processing are asynchronous. Repeat the calls for a
few seconds and inspect the application logs. Also use a new order ID; duplicate IDs intentionally do
not publish another `OrderCompleted` event.

### The internal publication exists but no EHS ID is found

First confirm that the publication exhausted its budget:

```bash
printf 'PUBLICATION_ID=%q\nERROR_TOKEN_LENGTH=%s\n' \
  "$PUBLICATION_ID" "${#ERROR_TOKEN}"

curl -sS \
  -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8090/jme-spring-modulith-scs/api/shipments/publications/$PUBLICATION_ID" | jq
```

The publication must be `FAILED` with three completion attempts before reconciliation emits the first
failure event. Then inspect the unfiltered EHS overview:

```bash
jq '.errors[] | {id, origin, eventId, eventName, errorState}' <<<"$EHS_JSON"
```

For a Modulith error, `eventId` in this overview equals the publication UUID. Do not filter the overview
on `publicationId`; that field exists only in `GET /api/error/{errorId}/details`. Do not filter on
`ModulithPublicationProcessingFailedEvent`; the exposed `eventName` is the internal event's fully
qualified type, such as `ch.admin.bit.jme.modulith.order.OrderCompleted`.

For local diagnosis, query the EHS database directly:

```bash
docker compose -f docker/docker-compose.yml exec -T \
  jeap-error-handling-service-db-local \
  psql -U errorhandling -d jeap-error-handling-service-db-local \
  -c "select e.id, e.state, ce.origin, ce.modulith_publication_id
        from data.error e
        join data.causing_event ce on ce.id = e.causing_event_id
       where ce.modulith_publication_id = '$PUBLICATION_ID'
       order by e.created desc;"
```

### Retry succeeded but the next error ID is empty

Retry closes the first EHS generation before the always-failing listener creates the next one. Wait for
the publication to reach `FAILED` with four completion attempts and rerun the second-generation lookup.

## Code tour

| File | Responsibility |
|---|---|
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/Application.java` | Modulith bootstrap, scheduling, business message contracts and retry/discard consumer contracts |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/messaging/DemoOrderPublisherController.java` | Demo Kafka producer and trace-ID response |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/messaging/OrderCreatedKafkaConsumer.java` | Kafka adapter and deliberate Kafka failure paths |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/order/OrderManagement.java` | Idempotent order persistence and `OrderCompleted` publication |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/inventory/InventoryManagement.java` | Asynchronous inventory listener |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/notification/NotificationManagement.java` | Asynchronous notification listener |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/shipping/ShippingManagement.java` | Asynchronous shipping listener, deliberate failure and attempt counter |
| `jme-spring-modulith-scs/src/main/java/ch/admin/bit/jme/modulith/shipping/ShippingController.java` | Shipment, attempt and durable publication inspection APIs |
| `jme-spring-modulith-test/src/test/java/ch/admin/bit/jme/modulith/test/InternalAsyncEventRetryIT.java` | Complete automatic retry, EHS generation, retry and discard flow |

## Configuration tour

| File | Responsibility |
|---|---|
| `jme-spring-modulith-scs/src/main/resources/application.yml` | Application identity, publication registry, retry policy, topics, OAuth and actuator |
| `jme-spring-modulith-scs/src/main/resources/application-local.yml` | Local port, Kafka, issuer and admin endpoints |
| `jme-spring-modulith-auth-scs/src/main/resources/application-local.yml` | OAuth clients, audiences, roles and browser user |
| `jme-spring-modulith-error-scs/src/main/resources/application.yml` | EHS topics, database and OAuth client |
| `jme-spring-modulith-error-scs/src/main/resources/application-local.yml` | Local EHS retry strategy and frontend |
| `docker/docker-compose.yml` | Kafka, Schema Registry and both PostgreSQL databases |
| `jme-spring-modulith-scs/src/main/resources/db/migration/V1__initial_schema.sql` | Complete schema: Spring Modulith JDBC v2 registry, application modules, failure generations, transactional outbox and ShedLock |
| `pom.xml` | Module list, business message and starter dependency versions |

See [Architecture](architecture.md) for the module and runtime design, [Configuration](configuration.md)
for the exact settings, and [Async event error handling](async-event-error-handling-design.md) for the
retry, escalation and operator-command design.
