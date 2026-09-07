package ch.admin.bit.jme.modulith.order;

/**
 * Published by the {@code order} module once an order has been registered and completed.
 * <p>
 * This is an <em>internal</em> application event: it never leaves the process, it is a plain Java
 * record rather than an Avro message, and it is delivered to the other application modules by Spring's
 * {@code ApplicationEventPublisher}. The listeners are annotated with
 * {@code @ApplicationModuleListener}, so delivery is asynchronous, happens in a transaction of its own
 * and is tracked in the Spring Modulith event publication registry.
 */
public record OrderCompleted(String orderId, String orderType) {
}
