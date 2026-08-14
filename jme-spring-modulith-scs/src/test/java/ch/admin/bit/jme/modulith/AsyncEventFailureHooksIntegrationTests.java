package ch.admin.bit.jme.modulith;

import ch.admin.bit.jme.modulith.order.OrderManagement;
import ch.admin.bit.jme.modulith.shipping.ShippingManagement;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.modulith.events.EventPublication;
import org.springframework.modulith.events.FailedEventPublications;
import org.springframework.modulith.events.ResubmissionOptions;
import org.springframework.modulith.events.core.EventPublicationRegistry;
import org.springframework.modulith.events.core.TargetEventPublication;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Demonstrates the Spring Modulith hooks the planned async event error handling bridge is going to
 * need, and proves that they behave as
 * {@code docs/async-event-error-handling-design.md} claims. Whoever implements that bridge should be
 * able to read this test instead of rediscovering the API.
 * <p>
 * Four things are shown, in the order the bridge will use them:
 * <ol>
 *     <li><b>Detect</b> — a listener that throws leaves a {@code FAILED} publication that can be
 *         enumerated, together with the event, the target listener and the attempt count.</li>
 *     <li><b>Escalate</b> — {@link EventPublicationRegistry#processFailedPublications} hands each
 *         failed publication to a callback <em>without</em> resubmitting it, which is what the bridge
 *         needs in order to publish an error event instead of retrying.</li>
 *     <li><b>Retry</b> — a single publication can be resubmitted by identifier, which is what
 *         handling a {@code RetryModulithPublicationCommand} amounts to.</li>
 *     <li><b>Discard</b> — a single publication can be marked completed without invoking the
 *         listener, which is what handling a {@code DiscardModulithPublicationCommand} amounts to.</li>
 * </ol>
 * The scheduled {@link FailedEventPublicationResubmitter} is switched off here (a one hour interval,
 * and a retry budget that is already used up after the first attempt) so that the test drives the
 * retries itself and nothing happens behind its back.
 */
@SpringBootTest(properties = {
        "jme.modulith.event-resubmission.interval=1h",
        "jme.modulith.event-resubmission.max-completion-attempts=1"
})
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class AsyncEventFailureHooksIntegrationTests {

    @Autowired
    OrderManagement orderManagement;

    @Autowired
    ShippingManagement shippingManagement;

    /**
     * The public API for resubmitting failed publications. It offers no query method — the only way to
     * see the failed publications through it is the filter predicate, which is called once per
     * candidate.
     */
    @Autowired
    FailedEventPublications failedEventPublications;

    /**
     * The lower-level registry. Unlike {@link FailedEventPublications} it can enumerate publications
     * and change their state, which is what the bridge needs for escalating and discarding.
     */
    @Autowired
    EventPublicationRegistry registry;

    @Test
    void failedListenerLeavesAFailedPublicationThatCanBeDetected() {

        String orderId = failingOrder();

        // Hook 1: the failed publication is enumerable, and carries everything an error report needs.
        TargetEventPublication publication = failedShippingPublication(orderId);

        assertThat(publication.getStatus()).isEqualTo(EventPublication.Status.FAILED);
        assertThat(publication.getEvent()).isInstanceOf(ch.admin.bit.jme.modulith.order.OrderCompleted.class);
        assertThat(publication.getTargetIdentifier().getValue())
                .contains("ShippingManagement.on");
        assertThat(publication.getIdentifier()).isNotNull();
        assertThat(publication.getPublicationDate()).isNotNull();

        // The listeners of the other modules were not affected by the failing one.
        assertThat(incompletePublicationsFor(orderId))
                .as("only the shipping listener failed")
                .hasSize(1);
    }

    @Test
    void failedPublicationsCanBeInspectedWithoutResubmittingThem() {

        String orderId = failingOrder();
        UUID publicationId = failedShippingPublication(orderId).getIdentifier();

        // Hook 2: processFailedPublications hands every failed publication to the callback. Doing
        // nothing in the callback means the publication is *not* resubmitted - which is exactly what
        // the bridge wants: report the failure, leave the publication alone until told otherwise.
        var seen = new java.util.ArrayList<UUID>();
        registry.processFailedPublications(ResubmissionOptions.defaults(),
                publication -> seen.add(publication.getIdentifier()));

        assertThat(seen).contains(publicationId);
        assertThat(shippingAttempts(orderId))
                .as("inspecting a failed publication must not invoke the listener again")
                .isEqualTo(1);
    }

    @Test
    void aSinglePublicationCanBeResubmittedByIdentifier() {

        String orderId = failingOrder();
        UUID publicationId = failedShippingPublication(orderId).getIdentifier();

        // Hook 3: retry exactly one publication - what RetryModulithPublicationCommand will do.
        resubmit(publicationId);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(shippingAttempts(orderId)).isEqualTo(2));

        // It failed again, so it is failed once more and its attempt count has gone up.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            TargetEventPublication resubmitted = failedShippingPublication(orderId);
            assertThat(resubmitted.getStatus()).isEqualTo(EventPublication.Status.FAILED);
            assertThat(resubmitted.getCompletionAttempts()).isEqualTo(2);
        });
    }

    @Test
    void aSinglePublicationCanBeDiscardedWithoutInvokingTheListener() {

        String orderId = failingOrder();
        TargetEventPublication publication = failedShippingPublication(orderId);

        // Hook 4: mark the publication completed without running the listener - what
        // DiscardModulithPublicationCommand will do.
        registry.markCompleted(publication.getEvent(), publication.getTargetIdentifier());

        assertThat(incompletePublicationsFor(orderId))
                .as("a discarded publication is no longer incomplete")
                .isEmpty();

        // A resubmission of everything that is still failed does not bring it back.
        failedEventPublications.resubmit(ResubmissionOptions.defaults());

        assertThat(shippingAttempts(orderId))
                .as("discarding must not invoke the listener")
                .isEqualTo(1);
    }

    /**
     * Registers an order whose shipping listener fails, and waits until the failure has been recorded.
     */
    private String failingOrder() {

        String orderId = UUID.randomUUID().toString();
        orderManagement.registerOrder(orderId, ShippingManagement.FAIL_ASYNC);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(shippingAttempts(orderId)).isEqualTo(1));
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(incompletePublicationsFor(orderId)).hasSize(1));

        return orderId;
    }

    private void resubmit(UUID publicationId) {
        failedEventPublications.resubmit(ResubmissionOptions.defaults()
                .withFilter(publication -> publication.getIdentifier().equals(publicationId)));
    }

    private TargetEventPublication failedShippingPublication(String orderId) {
        return incompletePublicationsFor(orderId).stream()
                .findFirst()
                .orElseThrow(() -> new AssertionError("No incomplete publication for order " + orderId));
    }

    private List<TargetEventPublication> incompletePublicationsFor(String orderId) {
        return registry.findIncompletePublications().stream()
                .filter(publication -> publication.getEvent() instanceof ch.admin.bit.jme.modulith.order.OrderCompleted event
                        && orderId.equals(event.orderId()))
                .toList();
    }

    private int shippingAttempts(String orderId) {
        return shippingManagement.attemptsByOrderId().getOrDefault(orderId, 0);
    }
}
