package ch.admin.bit.jme.modulith.shipping;

import java.time.Instant;

public record ShipmentSummary(String orderId, String orderType, Instant handedOverAt) {
}
