package ch.admin.bit.jme.modulith.notification;

import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The REST API of the {@code notification} module, authorized with the semantic role
 * {@code jme_@notification_#read}.
 */
@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
class NotificationController {

    private final NotificationManagement notificationManagement;

    @GetMapping
    @PreAuthorize("hasRole('notification', 'read')")
    List<NotificationSummary> listNotifications() {
        return notificationManagement.findAll();
    }
}
