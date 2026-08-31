package ch.admin.bit.jme.modulith.notification;

import ch.admin.bit.jme.modulith.notification.internal.Notification;
import ch.admin.bit.jme.modulith.notification.internal.NotificationRepository;
import ch.admin.bit.jme.modulith.order.OrderCompleted;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationManagement {

    private final NotificationRepository notifications;

    /**
     * The second listener on {@link OrderCompleted}. It is completely unaware of the one in the
     * {@code inventory} module: the two run concurrently, in separate transactions, and one failing
     * does not roll back or block the other.
     */
    @ApplicationModuleListener
    void on(OrderCompleted event) {

        if (notifications.existsByOrderId(event.orderId())) {
            log.info("Order {} has already been notified about, skipping", event.orderId());
            return;
        }

        notifications.save(new Notification(event.orderId(),
                "Order %s of type %s has been completed".formatted(event.orderId(), event.orderType())));
        log.info("Notified about order {}", event.orderId());
    }

    @Transactional(readOnly = true)
    public List<NotificationSummary> findAll() {
        return notifications.findAllNewestFirst().stream()
                .map(n -> new NotificationSummary(n.getOrderId(), n.getMessage(), n.getSentAt()))
                .toList();
    }
}
