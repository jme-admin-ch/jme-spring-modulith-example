package ch.admin.bit.jme.modulith.notification.internal;

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
@Table(name = "notification")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Notification {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "message", nullable = false)
    private String message;

    @Column(name = "sent_at", nullable = false)
    private Instant sentAt;

    public Notification(String orderId, String message) {
        this.id = UUID.randomUUID();
        this.orderId = orderId;
        this.message = message;
        this.sentAt = Instant.now();
    }
}
