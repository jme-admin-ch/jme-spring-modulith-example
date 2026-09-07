package ch.admin.bit.jme.modulith.inventory;

import java.time.Instant;

public record ReservationSummary(String orderId, String orderType, Instant reservedAt) {
}
