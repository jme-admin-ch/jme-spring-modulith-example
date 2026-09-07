package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import io.restassured.http.ContentType;
import io.restassured.response.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** End-to-end coverage of the complete failed internal publication lifecycle. */
class InternalAsyncEventRetryIT extends SpringModulithExampleITBase {

    private static final int AUTOMATIC_RETRY_BUDGET = 3;
    private static final String MODULITH_DB = databaseUrl(
            "jme-spring-modulith-db-local", 5540, "jme-spring-modulith-db-local");
    private static final String ERROR_DB = databaseUrl(
            "jeap-error-handling-service-db-local", 5541, "jeap-error-handling-service-db-local");

    @BeforeAll
    static void startServices() throws Exception {
        startAllServices();
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-scs",
                JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC);
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-error-scs",
                "jme-modulith-publication-processing-failed");
        KafkaConsumerGroupAwaiter.waitForAssignment(
                "JME-jme-spring-modulith-scs-jeap-modulith-publication-retry",
                "jme-retry-modulith-publication");
        KafkaConsumerGroupAwaiter.waitForAssignment(
                "JME-jme-spring-modulith-scs-jeap-modulith-publication-discard",
                "jme-discard-modulith-publication");
    }

    @Test
    void exhaustedPublicationCanBeRetriedOncePerGenerationAndDiscarded() {
        String serviceToken = accessToken();
        String errorToken = errorHandlingAccessToken();
        String orderId = UUID.randomUUID().toString();

        publishOrderCreatedEvent(serviceToken, orderId, "FAIL_ASYNC");

        await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> {
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET);
            Publication publication = publicationFor(orderId);
            assertThat(publication).isNotNull();
            assertThat(publication.status()).isEqualTo("FAILED");
            assertThat(publication.completionAttempts()).isEqualTo(AUTOMATIC_RETRY_BUDGET);
        });

        Publication exhausted = publicationFor(orderId);
        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
            assertThat(publicationStatusFor(serviceToken, exhausted.id())).isEqualTo("FAILED");
            List<EhsError> errors = errorsFor(exhausted.id());
            assertThat(errors).hasSize(1);
            assertThat(errors.getFirst().state()).isEqualTo("PERMANENT");
            assertThat(failureGenerationCount(exhausted.id(), AUTOMATIC_RETRY_BUDGET)).isEqualTo(1);
            assertFailureOutboxMessage(exhausted.id(), AUTOMATIC_RETRY_BUDGET);
        });

        EhsError firstGeneration = errorsFor(exhausted.id()).getFirst();
        assertEhsPublicationDetails(errorToken, firstGeneration.id(), exhausted.id(), orderId);

        assertThat(orderIdsOf(serviceToken, "/api/shipments")).doesNotContain(orderId);
        assertThat(orderIdsOf(serviceToken, "/api/orders")).contains(orderId);
        assertThat(orderIdsOf(serviceToken, "/api/inventory")).contains(orderId);
        assertThat(orderIdsOf(serviceToken, "/api/notifications")).contains(orderId);

        // Several retry/reconciliation cycles must neither bypass the exhausted budget nor duplicate its error.
        await().during(Duration.ofSeconds(12)).atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET);
            assertThat(publicationFor(orderId)).isEqualTo(exhausted);
            assertThat(errorsFor(exhausted.id())).hasSize(1);
            assertThat(failureGenerationCount(exhausted.id(), AUTOMATIC_RETRY_BUDGET)).isEqualTo(1);
        });

        given().baseUri(ERROR_SCS_BASE_URL)
                .auth().oauth2(errorToken)
                .when().post("/api/error/{errorId}/event/retry", firstGeneration.id())
                .then().statusCode(200);

        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            Publication retried = publicationFor(orderId);
            assertThat(retried.status()).isEqualTo("FAILED");
            assertThat(retried.completionAttempts()).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(errorsFor(exhausted.id())).hasSize(2);
            assertThat(failureGenerationCount(exhausted.id(), AUTOMATIC_RETRY_BUDGET + 1)).isEqualTo(1);
            assertFailureOutboxMessage(exhausted.id(), AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(errorState(firstGeneration.id()))
                    .as("the original EHS generation is closed after dispatching its retry command")
                    .isIn("PERMANENT_RETRIED", "RESOLVE_ON_MANUALTASK");
        });

        EhsError secondGeneration = errorsFor(exhausted.id()).stream()
                .filter(error -> !error.id().equals(firstGeneration.id()))
                .findFirst()
                .orElseThrow();

        // Force the already sent command through the EHS outbox relay once more. Its old failure-event
        // token no longer matches generation four, so the starter must acknowledge it without re-running.
        long oldRetryOutboxId = retryOutboxId(firstGeneration.id());
        duplicateOutboxMessage(oldRetryOutboxId);
        await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(wasRelayedByScheduler(oldRetryOutboxId)).isTrue());
        await().during(Duration.ofSeconds(12)).atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(publicationFor(orderId).completionAttempts()).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(errorsFor(exhausted.id())).hasSize(2);
        });

        given().baseUri(ERROR_SCS_BASE_URL)
                .auth().oauth2(errorToken)
                .queryParam("reason", "verified by InternalAsyncEventRetryIT")
                .when().delete("/api/error/{errorId}", secondGeneration.id())
                .then().statusCode(200);

        await().atMost(Duration.ofMinutes(1)).untilAsserted(() -> {
            Publication discarded = publicationFor(orderId);
            assertThat(discarded.status()).isEqualTo("COMPLETED");
            assertThat(discarded.completionAttempts()).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(publicationStatusFor(serviceToken, exhausted.id())).isEqualTo("COMPLETED");
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
        });

        await().during(Duration.ofSeconds(12)).atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(publicationFor(orderId).status()).isEqualTo("COMPLETED");
            assertThat(shippingAttemptsFor(serviceToken, orderId)).isEqualTo(AUTOMATIC_RETRY_BUDGET + 1);
            assertThat(errorsFor(exhausted.id())).hasSize(2);
            assertThat(totalFailureGenerationCount(exhausted.id())).isEqualTo(2);
        });
    }

    @Test
    void successfulInternalAsyncEventIsProcessedOnTheFirstAttempt() {
        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        publishOrderCreatedEvent(token, orderId, "STANDARD");

        await().untilAsserted(() -> assertThat(orderIdsOf(token, "/api/shipments")).contains(orderId));
        assertThat(shippingAttemptsFor(token, orderId)).isEqualTo(1);
    }

    private void assertEhsPublicationDetails(String token, UUID errorId, UUID publicationId, String orderId) {
        Response details = given().baseUri(ERROR_SCS_BASE_URL)
                .auth().oauth2(token)
                .accept(ContentType.JSON)
                .when().get("/api/error/{errorId}/details", errorId)
                .then().statusCode(200)
                .extract().response();

        assertThat(details.jsonPath().getString("origin")).isEqualTo("MODULITH_PUBLICATION");
        assertThat(details.jsonPath().getString("publicationId")).isEqualTo(publicationId.toString());
        assertThat(details.jsonPath().getString("publicationListener")).contains("ShippingManagement.on");
        assertThat(details.jsonPath().getString("publicationEventType")).endsWith(".OrderCompleted");
        assertThat(details.jsonPath().getString("publicationPayloadContentType")).isEqualTo("application/json");

        String payload = given().baseUri(ERROR_SCS_BASE_URL)
                .auth().oauth2(token)
                .when().get("/api/error/{errorId}/event/payload", errorId)
                .then().statusCode(200)
                .extract().asString();
        assertThat(payload).contains(orderId).contains("FAIL_ASYNC");
    }

    private int shippingAttemptsFor(String token, String orderId) {
        Integer attempts = get(token, "/api/shipments/attempts")
                .then().statusCode(200)
                .extract().jsonPath().getObject("'%s'".formatted(orderId), Integer.class);
        return attempts == null ? 0 : attempts;
    }

    private String publicationStatusFor(String token, UUID publicationId) {
        return get(token, "/api/shipments/publications/" + publicationId)
                .then().statusCode(200)
                .extract().jsonPath().getString("status");
    }

    private Publication publicationFor(String orderId) {
        String sql = """
                SELECT id, status, completion_attempts
                  FROM event_publication
                 WHERE listener_id LIKE '%ShippingManagement.on%'
                   AND serialized_event LIKE ?
                 ORDER BY publication_date DESC
                 LIMIT 1
                """;
        try (Connection connection = modulithConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, "%" + orderId + "%");
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? new Publication(result.getObject("id", UUID.class), result.getString("status"),
                        result.getInt("completion_attempts"))
                        : null;
            }
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot read the Modulith publication", exception);
        }
    }

    private List<EhsError> errorsFor(UUID publicationId) {
        String sql = """
                SELECT error.id, error.state
                  FROM data.error error
                  JOIN data.causing_event causing_event ON causing_event.id = error.causing_event_id
                 WHERE causing_event.origin = 'MODULITH_PUBLICATION'
                   AND causing_event.modulith_publication_id = ?
                 ORDER BY error.created
                """;
        List<EhsError> errors = new ArrayList<>();
        try (Connection connection = errorConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, publicationId.toString());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    errors.add(new EhsError(result.getObject("id", UUID.class), result.getString("state")));
                }
            }
            return errors;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot read EHS errors", exception);
        }
    }

    private String errorState(UUID errorId) {
        return queryString(ERROR_DB, "errorhandling", "SELECT state FROM data.error WHERE id = ?", errorId);
    }

    private int failureGenerationCount(UUID publicationId, int completionAttempts) {
        return queryInt(MODULITH_DB, "modulith", """
                SELECT count(*) FROM modulith_publication_failure
                 WHERE publication_id = ? AND completion_attempts = ?
                """, publicationId, completionAttempts);
    }

    private int totalFailureGenerationCount(UUID publicationId) {
        return queryInt(MODULITH_DB, "modulith",
                "SELECT count(*) FROM modulith_publication_failure WHERE publication_id = ?", publicationId);
    }

    private void assertFailureOutboxMessage(UUID publicationId, int completionAttempts) {
        FailureOutboxMessage message = failureOutboxMessage(publicationId, completionAttempts);
        assertThat(message).isNotNull();
        assertThat(message.topic()).isEqualTo("jme-modulith-publication-processing-failed");
        assertThat(message.messageId()).isEqualTo(message.errorEventId());
        assertThat(message.messageIdempotenceId()).isEqualTo(publicationId + ":" + completionAttempts);
        assertThat(message.messageTypeName()).isEqualTo("ModulithPublicationProcessingFailedEvent");
        assertThat(message.sendImmediately()).isTrue();
        assertThat(message.sentImmediately()).isNotNull();
    }

    private FailureOutboxMessage failureOutboxMessage(UUID publicationId, int completionAttempts) {
        String sql = """
                SELECT outbox.topic, outbox.message_id, outbox.message_idempotence_id,
                       outbox.message_type_name, outbox.send_immediately, outbox.sent_immediately,
                       failure.error_event_id
                  FROM deferred_message outbox
                  JOIN modulith_publication_failure failure
                    ON failure.error_event_id = outbox.message_id
                 WHERE failure.publication_id = ? AND failure.completion_attempts = ?
                """;
        try (Connection connection = modulithConnection();
             PreparedStatement statement = prepare(connection, sql, publicationId, completionAttempts);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                return null;
            }
            FailureOutboxMessage message = new FailureOutboxMessage(
                    result.getString("topic"),
                    result.getString("message_id"),
                    result.getString("message_idempotence_id"),
                    result.getString("message_type_name"),
                    result.getObject("send_immediately", Boolean.class),
                    result.getObject("sent_immediately", OffsetDateTime.class),
                    result.getString("error_event_id"));
            assertThat(result.next()).as("one outbox message per failure generation").isFalse();
            return message;
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot read the failure outbox message", exception);
        }
    }

    private long retryOutboxId(UUID errorId) {
        String sql = """
                SELECT id FROM data.deferred_message
                 WHERE message_type_name = 'RetryModulithPublicationCommand'
                   AND message_idempotence_id = ?
                 ORDER BY id DESC LIMIT 1
                """;
        return queryLong(ERROR_DB, "errorhandling", sql, "retry:" + errorId);
    }

    private void duplicateOutboxMessage(long id) {
        String sql = """
                UPDATE data.deferred_message
                   SET sent_immediately = NULL, sent_scheduled = NULL, send_immediately = FALSE,
                       schedule_after = NULL, failed = NULL, resend = FALSE
                 WHERE id = ?
                """;
        execute(ERROR_DB, "errorhandling", sql, id);
    }

    private boolean wasRelayedByScheduler(long id) {
        return queryInt(ERROR_DB, "errorhandling", """
                SELECT count(*) FROM data.deferred_message
                 WHERE id = ? AND sent_scheduled IS NOT NULL
                """, id) == 1;
    }

    private static int queryInt(String url, String user, String sql, Object... arguments) {
        try (Connection connection = DriverManager.getConnection(url, user, "secret");
             PreparedStatement statement = prepare(connection, sql, arguments);
             ResultSet result = statement.executeQuery()) {
            result.next();
            return result.getInt(1);
        } catch (Exception exception) {
            throw new IllegalStateException("Database query failed", exception);
        }
    }

    private static long queryLong(String url, String user, String sql, Object... arguments) {
        try (Connection connection = DriverManager.getConnection(url, user, "secret");
             PreparedStatement statement = prepare(connection, sql, arguments);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                throw new IllegalStateException("Database query returned no row");
            }
            return result.getLong(1);
        } catch (Exception exception) {
            throw new IllegalStateException("Database query failed", exception);
        }
    }

    private static String queryString(String url, String user, String sql, Object... arguments) {
        try (Connection connection = DriverManager.getConnection(url, user, "secret");
             PreparedStatement statement = prepare(connection, sql, arguments);
             ResultSet result = statement.executeQuery()) {
            if (!result.next()) {
                return null;
            }
            return result.getString(1);
        } catch (Exception exception) {
            throw new IllegalStateException("Database query failed", exception);
        }
    }

    private static void execute(String url, String user, String sql, Object... arguments) {
        try (Connection connection = DriverManager.getConnection(url, user, "secret");
             PreparedStatement statement = prepare(connection, sql, arguments)) {
            assertThat(statement.executeUpdate()).isEqualTo(1);
        } catch (Exception exception) {
            throw new IllegalStateException("Database update failed", exception);
        }
    }

    private static PreparedStatement prepare(Connection connection, String sql, Object... arguments)
            throws Exception {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int index = 0; index < arguments.length; index++) {
            statement.setObject(index + 1, arguments[index]);
        }
        return statement;
    }

    private static Connection modulithConnection() throws Exception {
        return DriverManager.getConnection(MODULITH_DB, "modulith", "secret");
    }

    private static Connection errorConnection() throws Exception {
        return DriverManager.getConnection(ERROR_DB, "errorhandling", "secret");
    }

    private static String databaseUrl(String ciHost, int localPort, String database) {
        String host = System.getenv("CI") == null ? "localhost:" + localPort : ciHost + ":5432";
        return "jdbc:postgresql://%s/%s".formatted(host, database);
    }

    private record Publication(UUID id, String status, int completionAttempts) {
    }

    private record EhsError(UUID id, String state) {
    }

    private record FailureOutboxMessage(String topic, String messageId, String messageIdempotenceId,
                                        String messageTypeName, Boolean sendImmediately,
                                        OffsetDateTime sentImmediately, String errorEventId) {
    }
}
