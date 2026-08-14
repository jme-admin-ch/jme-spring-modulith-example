package ch.admin.bit.jme.modulith.order.internal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * An order. Module-internal: no other application module ever sees this type, they work with
 * {@link ch.admin.bit.jme.modulith.order.OrderSummary} instead.
 */
@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Order {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "order_type", nullable = false)
    private String orderType;

    @Column(name = "completed_at", nullable = false)
    private Instant completedAt;

    public Order(String orderId, String orderType) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.orderType = orderType;
        this.completedAt = Instant.now();
    }
}
