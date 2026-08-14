package ch.admin.bit.jme.modulith.order;

import ch.admin.bit.jme.modulith.order.internal.Order;
import ch.admin.bit.jme.modulith.order.internal.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

/**
 * The API of the {@code order} module. Everything that wants to create or read orders goes through
 * this service — the module's own REST controller as well as the Kafka adapter in the
 * {@code messaging} module.
 */
@Service
@RequiredArgsConstructor
@Transactional
@Slf4j
public class OrderManagement {

    private final OrderRepository orders;
    private final ApplicationEventPublisher events;

    /**
     * Registers an order and announces it to the rest of the application with an {@link OrderCompleted}
     * event.
     * <p>
     * The operation is idempotent: Kafka delivers at least once, and the error handling service may
     * resend a message that already got as far as being persisted, so an order that is already known
     * is returned unchanged and no second event is published.
     */
    public OrderSummary registerOrder(String orderId, String orderType) {

        Optional<Order> existing = orders.findByOrderId(orderId);
        if (existing.isPresent()) {
            log.info("Order {} is already registered, skipping", orderId);
            return OrderSummary.of(existing.get());
        }

        Order order = orders.save(new Order(orderId, orderType));
        log.info("Registered order {} of type {}", order.getOrderId(), order.getOrderType());

        // Published within the current transaction. The @ApplicationModuleListener methods in the
        // other modules only run once this transaction has committed, each in a transaction of its
        // own, and the Spring Modulith event publication registry keeps a row per listener until it
        // has completed successfully.
        events.publishEvent(new OrderCompleted(order.getOrderId(), order.getOrderType()));

        return OrderSummary.of(order);
    }

    @Transactional(readOnly = true)
    public List<OrderSummary> findAll() {
        return orders.findAllNewestFirst().stream().map(OrderSummary::of).toList();
    }

    @Transactional(readOnly = true)
    public Optional<OrderSummary> findByOrderId(String orderId) {
        return orders.findByOrderId(orderId).map(OrderSummary::of);
    }
}
