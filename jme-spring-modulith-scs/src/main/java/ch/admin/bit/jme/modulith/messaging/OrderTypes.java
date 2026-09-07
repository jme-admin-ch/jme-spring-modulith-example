package ch.admin.bit.jme.modulith.messaging;

/**
 * Order types the Kafka adapter treats specially, so that the error handling of this example can be
 * demonstrated without having to break anything.
 */
final class OrderTypes {

    /** Makes the consumer fail with a temporary error, which the error handling service resends. */
    static final String FAIL_TEMPORARY = "FAIL_TEMPORARY";

    /** Makes the consumer fail with a permanent error, which waits for a manual decision. */
    static final String FAIL_PERMANENT = "FAIL_PERMANENT";

    private OrderTypes() {
    }
}
