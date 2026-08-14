package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Covers the failure path of an <em>internal</em> asynchronous event, as opposed to the Kafka
 * consumption failures covered by {@link ErrorHandlingIT}.
 * <p>
 * An order of type {@code FAIL_ASYNC} is consumed from Kafka without trouble and registered in the
 * {@code order} module, which publishes {@code OrderCompleted}. The {@code shipping} listener of that
 * event then fails, so Spring Modulith marks its event publication {@code FAILED} and the resubmission
 * configured in {@code FailedEventPublicationResubmitter} retries it until the retry budget
 * ({@code jme.modulith.event-resubmission.max-completion-attempts}) is used up.
 * <p>
 * The listeners of the other two modules complete normally — each listener has its own publication and
 * its own transaction, so one of them failing does not hold up the others.
 */
class InternalAsyncEventRetryIT extends SpringModulithExampleITBase {

    /**
     * Spring Modulith counts the initial invocation as the first completion attempt, so
     * jme.modulith.event-resubmission.max-completion-attempts=3 means the listener runs three times in
     * total: the initial attempt plus two retries.
     */
    private static final int EXPECTED_TOTAL_ATTEMPTS = 3;

    @BeforeAll
    static void startServices() throws Exception {
        startAllServices();
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-scs",
                JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC);
    }

    @Test
    void failedInternalAsyncEventIsRetriedUntilTheRetryBudgetIsUsedUp() {

        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        publishOrderCreatedEvent(token, orderId, "FAIL_ASYNC");

        // The shipping listener is invoked once and then resubmitted until the budget is used up.
        await().atMost(Duration.ofMinutes(2))
                .untilAsserted(() -> assertThat(shippingAttemptsFor(token, orderId))
                        .isEqualTo(EXPECTED_TOTAL_ATTEMPTS));

        // Nothing was shipped, and the retries stop rather than running forever.
        assertThat(orderIdsOf(token, "/api/shipments")).doesNotContain(orderId);
        await().during(Duration.ofSeconds(20))
                .atMost(Duration.ofSeconds(40))
                .untilAsserted(() -> assertThat(shippingAttemptsFor(token, orderId))
                        .isEqualTo(EXPECTED_TOTAL_ATTEMPTS));

        // The order itself was registered, and the listeners of the other modules were not affected by
        // the failing one.
        assertThat(orderIdsOf(token, "/api/orders")).contains(orderId);
        assertThat(orderIdsOf(token, "/api/inventory")).contains(orderId);
        assertThat(orderIdsOf(token, "/api/notifications")).contains(orderId);
    }

    @Test
    void successfulInternalAsyncEventIsProcessedOnTheFirstAttempt() {

        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        publishOrderCreatedEvent(token, orderId, "STANDARD");

        await().untilAsserted(() -> assertThat(orderIdsOf(token, "/api/shipments")).contains(orderId));
        assertThat(shippingAttemptsFor(token, orderId)).isEqualTo(1);
    }

    private int shippingAttemptsFor(String token, String orderId) {
        Integer attempts = get(token, "/api/shipments/attempts")
                .then().statusCode(200)
                .extract().jsonPath().getObject("'%s'".formatted(orderId), Integer.class);
        return attempts == null ? 0 : attempts;
    }
}
