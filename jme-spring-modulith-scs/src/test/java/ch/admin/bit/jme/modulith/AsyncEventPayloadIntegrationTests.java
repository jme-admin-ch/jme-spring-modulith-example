package ch.admin.bit.jme.modulith;

import ch.admin.bit.jme.modulith.order.OrderCompleted;
import ch.admin.bit.jme.modulith.order.OrderManagement;
import ch.admin.bit.jme.modulith.shipping.ShippingManagement;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.modulith.events.core.EventPublicationRegistry;
import org.springframework.modulith.events.core.EventSerializer;
import org.springframework.modulith.events.core.TargetEventPublication;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Establishes how the planned error handling bridge can put the failed event's payload into the
 * {@code ModulithPublicationProcessingFailedEvent}, so the Error Handling Service can show an operator
 * what actually failed.
 * <p>
 * The question this answers is <em>where the payload comes from</em>. {@code TargetEventPublication}
 * exposes the event as a deserialized object, not as the stored text, so the obvious route —
 * {@code getEvent()} — depends on the event class still being loadable and deserializable. The clean
 * alternative asserted here is Spring Modulith's own {@link EventSerializer}: it is the extension point
 * that produced the stored form in the first place, it is a public bean, and running it over the live
 * event object yields **exactly the bytes stored in {@code event_publication.serialized_event}**. No
 * JDBC access and no deserialization are needed. The bridge reports the payload as
 * {@code application/json}, matching Spring Modulith's default Jackson serializer.
 * <p>
 * The extraction is best effort: an event that cannot be serialized produces no payload rather than a
 * failed escalation, and an oversized payload is truncated. See
 * {@code docs/async-event-error-handling-design.md}.
 */
@SpringBootTest(properties = {
        "jme.modulith.event-resubmission.interval=1h",
        "jme.modulith.event-resubmission.max-completion-attempts=1"
})
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class AsyncEventPayloadIntegrationTests {

    /**
     * Kafka's default maximum message size is 1 MB, and the payload is only one field of the failure
     * event, so it is capped well below that.
     */
    private static final int MAX_PAYLOAD_BYTES = 256 * 1024;

    @Autowired
    OrderManagement orderManagement;

    @Autowired
    EventPublicationRegistry registry;

    @Autowired
    EventSerializer eventSerializer;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void serializingTheLiveEventYieldsExactlyWhatTheRegistryStored() {

        String orderId = UUID.randomUUID().toString();
        orderManagement.registerOrder(orderId, ShippingManagement.FAIL_ASYNC);

        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(publicationsFor(orderId)).hasSize(1));

        TargetEventPublication publication = publicationsFor(orderId).getFirst();
        String stored = jdbc.queryForObject(
                "SELECT serialized_event FROM event_publication WHERE id = ?", String.class,
                publication.getIdentifier());

        byte[] extracted = payloadOf(new OrderCompleted(orderId, ShippingManagement.FAIL_ASYNC)).orElseThrow();

        assertThat(new String(extracted, StandardCharsets.UTF_8)).isEqualTo(stored);
    }

    @Test
    void theExtractedPayloadIsJsonDescribingTheEvent() {

        String orderId = UUID.randomUUID().toString();

        String payload = new String(payloadOf(new OrderCompleted(orderId, "STANDARD")).orElseThrow(),
                StandardCharsets.UTF_8);

        assertThat(payload).startsWith("{").endsWith("}").contains(orderId).contains("STANDARD");
    }

    @Test
    void anEventThatCannotBeSerializedYieldsNoPayloadInsteadOfAnError() {

        // The escalation must still go out for an event the serializer chokes on - without the
        // payload, and with the reason in the log. (A bare Object would not do as an example: Jackson
        // serializes it to "{}" rather than failing.)
        assertThat(payloadOf(new Unserializable())).isEmpty();
    }

    /** An event whose serialization fails, because reading its property throws. */
    static class Unserializable {

        @SuppressWarnings("unused")
        public String getBoom() {
            throw new IllegalStateException("this property cannot be read");
        }
    }

    @Test
    void anOversizedPayloadIsTruncated() {

        String hugeOrderType = "X".repeat(MAX_PAYLOAD_BYTES);

        byte[] payload = payloadOf(new OrderCompleted("4711", hugeOrderType)).orElseThrow();

        assertThat(payload).hasSize(MAX_PAYLOAD_BYTES);
    }

    /**
     * What the bridge will do, in the shape it will do it: serialize with Spring Modulith's own
     * serializer, cap the size, and give up quietly if that is not possible.
     */
    private Optional<byte[]> payloadOf(Object event) {
        try {
            byte[] bytes = eventSerializer.serialize(event).toString().getBytes(StandardCharsets.UTF_8);
            return Optional.of(bytes.length > MAX_PAYLOAD_BYTES
                    ? Arrays.copyOf(bytes, MAX_PAYLOAD_BYTES)
                    : bytes);
        } catch (Exception e) {
            // The starter logs here; the failure event is published without a payload.
            return Optional.empty();
        }
    }

    private List<TargetEventPublication> publicationsFor(String orderId) {
        return registry.findIncompletePublications().stream()
                .filter(publication -> publication.getEvent() instanceof OrderCompleted event
                        && orderId.equals(event.orderId()))
                .toList();
    }
}
