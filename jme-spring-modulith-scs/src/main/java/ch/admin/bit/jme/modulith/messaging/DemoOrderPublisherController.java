package ch.admin.bit.jme.modulith.messaging;

import ch.admin.bit.jeap.messaging.avro.AvroMessage;
import ch.admin.bit.jeap.messaging.avro.AvroMessageKey;
import ch.admin.bit.jeap.messaging.kafka.tracing.TraceContextProvider;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Publishes a {@code JmeOrderCreatedEvent} to Kafka so the example can be exercised without a second
 * system. Everything downstream of the topic — the consumer, the error handling, the internal events —
 * behaves exactly as it would with a real external producer.
 */
@RestController
@RequestMapping("/api/demo/orders")
@RequiredArgsConstructor
@Slf4j
class DemoOrderPublisherController {

    private static final int SEND_TIMEOUT_SEC = 30;

    private final KafkaTemplate<AvroMessageKey, AvroMessage> kafkaTemplate;
    private final TraceContextProvider traceContextProvider;

    /**
     * @param orderType {@code FAIL_TEMPORARY} and {@code FAIL_PERMANENT} make the consumer fail on
     *                  purpose, see {@link OrderTypes}. Any other value takes the happy path.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    @PreAuthorize("hasRole('order', 'write')")
    PublishedEvent publish(@RequestParam String orderId,
                           @RequestParam(defaultValue = "STANDARD") String orderType) throws Exception {

        JmeOrderCreatedEvent event = JmeOrderCreatedEventBuilder.create()
                .idempotenceId(UUID.randomUUID().toString())
                .orderId(orderId)
                .orderType(orderType)
                .build();

        kafkaTemplate.send(JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC, event)
                .get(SEND_TIMEOUT_SEC, TimeUnit.SECONDS);
        log.info("Published JmeOrderCreatedEvent {} for order {} of type {}",
                event.getIdentity().getEventId(), orderId, orderType);

        // The trace id ties the whole flow together: this call, the consumption of the event, and — if
        // processing fails — the entry the error handling service creates for it.
        return new PublishedEvent(event.getIdentity().getEventId(),
                JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC, orderId, orderType,
                traceContextProvider.getTraceContext().getTraceIdString());
    }

    record PublishedEvent(String eventId, String topic, String orderId, String orderType, String traceId) {
    }
}
