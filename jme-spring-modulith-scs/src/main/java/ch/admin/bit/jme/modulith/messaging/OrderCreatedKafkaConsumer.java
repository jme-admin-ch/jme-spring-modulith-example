package ch.admin.bit.jme.modulith.messaging;

import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import ch.admin.bit.jme.modulith.order.OrderManagement;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Consumes the external {@code JmeOrderCreatedEvent} and translates it into a call on the API of the
 * {@code order} module.
 * <p>
 * jEAP messaging runs the listener with {@code enable.auto.commit=false} and {@code AckMode=MANUAL},
 * so the offset is only committed once {@link Acknowledgment#acknowledge()} has been called. When the
 * listener throws, the jEAP error handler forwards the failure to the topic configured as
 * {@code jeap.messaging.kafka.errorTopicName} and acknowledges the record itself — the error handling
 * service owns the message from that point on.
 */
@Component
@RequiredArgsConstructor
@Slf4j
class OrderCreatedKafkaConsumer {

    private final OrderManagement orderManagement;

    @KafkaListener(topics = MessagingTopics.ORDER_CREATED)
    void consume(JmeOrderCreatedEvent event, Acknowledgment ack) {

        String orderId = event.getReferences().getReference().getOrderId();
        String orderType = event.getPayload().getOrderType();
        log.info("Received JmeOrderCreatedEvent {} for order {} of type {}",
                event.getIdentity().getEventId(), orderId, orderType);

        switch (orderType) {
            case OrderTypes.FAIL_TEMPORARY -> throw new TemporaryOrderProcessingException(orderId);
            case OrderTypes.FAIL_PERMANENT -> throw new PermanentOrderProcessingException(orderId);
            default -> orderManagement.registerOrder(orderId, orderType);
        }

        ack.acknowledge();
    }
}
