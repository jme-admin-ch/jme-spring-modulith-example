package ch.admin.bit.jme.modulith.shipping.internal;

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
@Table(name = "shipment")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Shipment {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "order_type", nullable = false)
    private String orderType;

    @Column(name = "handed_over_at", nullable = false)
    private Instant handedOverAt;

    public Shipment(String orderId, String orderType) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.orderType = orderType;
        this.handedOverAt = Instant.now();
    }
}
