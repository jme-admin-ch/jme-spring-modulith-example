package ch.admin.bit.jme.modulith.messaging;

/** Kafka topics owned by this example, separate from other users of the shared message types. */
public final class MessagingTopics {

    public static final String ORDER_CREATED = "jme-order-created-modulith";

    private MessagingTopics() {
    }
}
