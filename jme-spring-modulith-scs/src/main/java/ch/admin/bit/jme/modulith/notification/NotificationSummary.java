package ch.admin.bit.jme.modulith.notification;

import java.time.Instant;

public record NotificationSummary(String orderId, String message, Instant sentAt) {
}
