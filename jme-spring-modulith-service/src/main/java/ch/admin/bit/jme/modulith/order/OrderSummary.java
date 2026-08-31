package ch.admin.bit.jme.modulith.order;

import ch.admin.bit.jme.modulith.order.internal.Order;

import java.time.Instant;

/**
 * The view of an order the {@code order} module exposes — to its own REST endpoints as well as to the
 * other application modules. Keeping the JPA entity internal means a change to the persistence model
 * cannot ripple through the rest of the application.
 */
public record OrderSummary(String orderId, String orderType, Instant completedAt) {

    static OrderSummary of(Order order) {
        return new OrderSummary(order.getOrderId(), order.getOrderType(), order.getCompletedAt());
    }
}
