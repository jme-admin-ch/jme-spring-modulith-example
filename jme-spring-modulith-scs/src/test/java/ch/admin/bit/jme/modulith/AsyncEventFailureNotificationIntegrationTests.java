package ch.admin.bit.jme.modulith;

import ch.admin.bit.jme.modulith.order.OrderManagement;
import ch.admin.bit.jme.modulith.shipping.ShippingManagement;
import org.junit.jupiter.api.Test;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.modulith.events.EventPublication;
import org.springframework.modulith.events.FailedEventPublications;
import org.springframework.modulith.events.ResubmissionOptions;
import org.springframework.modulith.events.core.EventPublicationRegistry;
import org.springframework.modulith.events.core.TargetEventPublication;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.test.context.ActiveProfiles;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Shows that a failing internal asynchronous event can be reacted to <em>proactively</em>, without
 * polling the event publication registry.
 * <p>
 * Spring Modulith publishes no "publication failed" application event. What it does do is run every
 * {@code @ApplicationModuleListener} through Spring's {@code @Async} infrastructure, and an exception
 * escaping such a listener is handed to the {@link AsyncUncaughtExceptionHandler} — a plain Spring
 * extension point an application or a starter can replace. That call is the proactive hook.
 * <p>
 * Two properties of it are asserted here because the design depends on them:
 * <ol>
 *     <li>The handler is called with the failing listener method and the event, so the publication it
 *         belongs to can be identified.</li>
 *     <li>By the time it is called, Spring Modulith has <em>already</em> marked the publication
 *         {@code FAILED} — {@code CompletionRegisteringAdvisor} does that before letting the exception
 *         escape — so the handler can read the current attempt count and decide whether this was the
 *         last attempt.</li>
 * </ol>
 * It also shows the limit of the hook: it fires on <em>every</em> failed attempt, including retries, so
 * "retries exhausted" is a decision the handler has to make itself.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, AsyncEventFailureNotificationIntegrationTests.RecordingAsyncFailures.class})
@ActiveProfiles("test")
class AsyncEventFailureNotificationIntegrationTests {

    @Autowired
    OrderManagement orderManagement;

    @Autowired
    ShippingManagement shippingManagement;

    @Autowired
    FailedEventPublications failedEventPublications;

    @Autowired
    EventPublicationRegistry registry;

    @Autowired
    FailureRecorder recorded;

    @Test
    void aFailingListenerNotifiesTheAsyncExceptionHandlerWithTheEventAndTheListener() {

        String orderId = UUID.randomUUID().toString();
        orderManagement.registerOrder(orderId, ShippingManagement.FAIL_ASYNC);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(failuresFor(orderId)).hasSize(1));

        Failure failure = failuresFor(orderId).getFirst();
        assertThat(failure.method().getName()).isEqualTo("on");
        assertThat(failure.method().getDeclaringClass()).isEqualTo(ShippingManagement.class);
        assertThat(failure.exception().getMessage()).contains(orderId);
    }

    @Test
    void thePublicationIsAlreadyMarkedFailedWhenTheHandlerIsNotified() {

        String orderId = UUID.randomUUID().toString();
        orderManagement.registerOrder(orderId, ShippingManagement.FAIL_ASYNC);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(failuresFor(orderId)).hasSize(1));

        // The handler ran after CompletionRegisteringAdvisor marked the publication, so everything the
        // bridge needs to decide "was this the last attempt?" is readable at that moment.
        TargetEventPublication publication = publicationFor(orderId);
        assertThat(publication.getStatus()).isEqualTo(EventPublication.Status.FAILED);
        assertThat(publication.getCompletionAttempts()).isEqualTo(1);
    }

    @Test
    void theHandlerIsNotifiedAgainForEveryRetry() {

        String orderId = UUID.randomUUID().toString();
        orderManagement.registerOrder(orderId, ShippingManagement.FAIL_ASYNC);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(failuresFor(orderId)).hasSize(1));

        UUID publicationId = publicationFor(orderId).getIdentifier();
        failedEventPublications.resubmit(ResubmissionOptions.defaults()
                .withFilter(publication -> publication.getIdentifier().equals(publicationId)));

        // A retry that fails again is another notification, not a separate kind of event. The hook says
        // "this attempt failed", never "this publication is now given up on".
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(failuresFor(orderId)).hasSize(2));
        assertThat(publicationFor(orderId).getCompletionAttempts()).isEqualTo(2);
    }

    private List<Failure> failuresFor(String orderId) {
        return recorded.failures.stream()
                .filter(failure -> failure.exception().getMessage() != null
                        && failure.exception().getMessage().contains(orderId))
                .toList();
    }

    private TargetEventPublication publicationFor(String orderId) {
        return registry.findIncompletePublications().stream()
                .filter(publication -> publication.getEvent() instanceof ch.admin.bit.jme.modulith.order.OrderCompleted event
                        && orderId.equals(event.orderId()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No incomplete publication for order " + orderId));
    }

    record Failure(Throwable exception, Method method, Object event) {
    }

    static class FailureRecorder {

        final List<Failure> failures = new CopyOnWriteArrayList<>();

        void record(Throwable exception, Method method, Object... params) {
            failures.add(new Failure(exception, method, params.length > 0 ? params[0] : null));
        }
    }

    /**
     * Replaces Spring's default {@code SimpleAsyncUncaughtExceptionHandler}, which only logs, with one
     * that records what it was told. A starter would escalate here instead of recording.
     * <p>
     * Spring Boot wraps an application-provided {@link AsyncConfigurer} in its own
     * {@code ApplicationTaskExecutorAsyncConfigurer} and delegates the handler lookup to it, so
     * contributing one is the supported way to replace the handler. It has to be a separate bean
     * rather than the configuration class itself, whose bean definition Boot's own configurer
     * displaces.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class RecordingAsyncFailures {

        @Bean
        FailureRecorder failureRecorder() {
            return new FailureRecorder();
        }

        @Bean
        AsyncConfigurer recordingAsyncConfigurer(FailureRecorder recorder) {
            return new AsyncConfigurer() {
                @Override
                public AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
                    return recorder::record;
                }
            };
        }
    }
}
