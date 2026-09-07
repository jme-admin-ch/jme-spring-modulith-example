package ch.admin.bit.jme.modulith.notification.internal;

import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, UUID> {

    boolean existsByOrderId(String orderId);

    default List<Notification> findAllNewestFirst() {
        return findAll(Sort.by(Sort.Direction.DESC, "sentAt"));
    }
}
