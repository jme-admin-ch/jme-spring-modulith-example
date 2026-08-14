package ch.admin.bit.jme.modulith.messaging;

import ch.admin.bit.jeap.domainevent.avro.AvroDomainEventBuilder;
import ch.admin.bit.jme.messaging.OrderReference;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedPayload;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedReferences;
import lombok.Getter;

/**
 * Builds the Avro event the demo endpoint publishes. In a real deployment this builder would live in
 * the producing system, not here.
 */
@Getter
class JmeOrderCreatedEventBuilder extends AvroDomainEventBuilder<JmeOrderCreatedEventBuilder, JmeOrderCreatedEvent> {

    private final String systemName = "JME";
    private final String eventName = "JmeOrderCreatedEvent";
    private final String serviceName = "jme-spring-modulith-scs";

    private String orderId;
    private String orderType;

    private JmeOrderCreatedEventBuilder() {
        super(JmeOrderCreatedEvent::new);
    }

    static JmeOrderCreatedEventBuilder create() {
        return new JmeOrderCreatedEventBuilder();
    }

    JmeOrderCreatedEventBuilder orderId(String orderId) {
        this.orderId = orderId;
        return this;
    }

    JmeOrderCreatedEventBuilder orderType(String orderType) {
        this.orderType = orderType;
        return this;
    }

    @Override
    protected JmeOrderCreatedEventBuilder self() {
        return this;
    }

    @Override
    public JmeOrderCreatedEvent build() {
        setReferences(JmeOrderCreatedReferences.newBuilder()
                .setReference(OrderReference.newBuilder()
                        .setType("order")
                        .setOrderId(orderId)
                        .build())
                .build());
        setPayload(JmeOrderCreatedPayload.newBuilder()
                .setOrderType(orderType)
                .build());
        setProcessId(orderId);
        return super.build();
    }
}
