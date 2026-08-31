package ch.admin.bit.jme.modulith.inventory.internal;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "stock_reservation")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class StockReservation {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "order_type", nullable = false)
    private String orderType;

    @Column(name = "reserved_at", nullable = false)
    private Instant reservedAt;

    public StockReservation(String orderId, String orderType) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.orderType = orderType;
        this.reservedAt = Instant.now();
    }
}
