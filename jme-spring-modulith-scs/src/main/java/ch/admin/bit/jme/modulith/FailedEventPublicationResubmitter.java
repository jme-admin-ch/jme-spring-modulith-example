package ch.admin.bit.jme.modulith;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.modulith.events.EventPublication;
import org.springframework.modulith.events.FailedEventPublications;
import org.springframework.modulith.events.ResubmissionOptions;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Retries internal asynchronous events whose processing failed.
 * <p>
 * When an {@code @ApplicationModuleListener} throws, Spring Modulith marks that listener's row in
 * {@code event_publication} as {@code FAILED}. Spring Modulith persists such publications but does not
 * retry them on a schedule by itself — it offers {@link FailedEventPublications#resubmit(ResubmissionOptions)}
 * and leaves the retry policy to the application. This component is that policy: every
 * {@code jme.modulith.event-resubmission.interval}, failed publications that are old enough and have
 * not yet used up their retry budget are resubmitted, which invokes the listener again and increments
 * the publication's {@code completion_attempts}.
 * <p>
 * A publication that keeps failing therefore climbs to {@link #maxCompletionAttempts} attempts and is
 * then left alone, staying {@code FAILED} in the registry. That moment — <em>retries exhausted</em> —
 * is the extension point for the planned error handling bridge:
 * <pre>
 * Kafka event
 *   → internal async event
 *     → async event processor fails
 *       → Spring Modulith resubmission (this class)
 *         → retries exhausted
 *           → bridge publishes ModulithPublicationProcessingFailedEvent
 *             → jEAP Error Handling Service
 *               → RetryModulithPublicationCommand / DiscardModulithPublicationCommand
 *                 → this application resubmits or discards the publication
 * </pre>
 * Only the part up to "retries exhausted" exists today; the bridge is not implemented yet.
 * {@link #onRetriesExhausted(EventPublication)} is where it will hook in, and the {@code shipping}
 * module provides the failing listener needed to exercise it — see
 * {@link ch.admin.bit.jme.modulith.shipping.ShippingManagement#FAIL_ASYNC}.
 * <p>
 * This is a different path from the one the {@code messaging} module takes: a Kafka message whose
 * consumption fails synchronously is escalated to the error handling service by the jEAP error handler
 * right away, without ever reaching the event publication registry.
 */
@Component
@RequiredArgsConstructor
@Slf4j
class FailedEventPublicationResubmitter {

    private final FailedEventPublications failedEventPublications;

    /**
     * How often a listener may be invoked in total, the initial attempt included. Spring Modulith
     * counts the initial invocation as the first completion attempt, so a value of 3 means the initial
     * attempt plus two retries.
     */
    @Value("${jme.modulith.event-resubmission.max-completion-attempts}")
    private int maxCompletionAttempts;

    /**
     * How old a failed publication has to be before it is retried, so that a listener failing on a
     * transient hiccup is not retried in the same instant.
     */
    @Value("${jme.modulith.event-resubmission.min-age}")
    private Duration minAge;

    /**
     * Publications whose exhaustion has already been reported. Without it, every scheduled run would
     * report the same exhausted publication again — and the error handling bridge that will replace
     * the logging in {@link #onRetriesExhausted(EventPublication)} has to escalate a failure once, not
     * once every few seconds.
     */
    private final Set<UUID> exhausted = ConcurrentHashMap.newKeySet();

    @Scheduled(fixedDelayString = "${jme.modulith.event-resubmission.interval}")
    void resubmitFailedPublications() {
        failedEventPublications.resubmit(ResubmissionOptions.defaults()
                .withMinAge(minAge)
                .withFilter(this::hasRetriesLeft));
    }

    private boolean hasRetriesLeft(EventPublication publication) {

        if (publication.getCompletionAttempts() < maxCompletionAttempts) {
            return true;
        }

        if (exhausted.add(publication.getIdentifier())) {
            onRetriesExhausted(publication);
        }
        return false;
    }

    /**
     * Called once per publication that has used up its retry budget. Today it only reports the failure;
     * this is the seam where the error handling bridge will publish a
     * {@code ModulithPublicationProcessingFailedEvent} towards the jEAP Error Handling Service, so that
     * the failure can be retried or discarded from there.
     */
    private void onRetriesExhausted(EventPublication publication) {
        log.error("Event publication {} of event {} failed {} times and is not retried again. "
                        + "It stays FAILED in the event publication registry.",
                publication.getIdentifier(), publication.getEvent().getClass().getName(),
                publication.getCompletionAttempts());
    }
}
