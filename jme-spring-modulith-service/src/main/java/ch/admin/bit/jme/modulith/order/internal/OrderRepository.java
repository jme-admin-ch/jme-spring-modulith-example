package ch.admin.bit.jme.modulith.order.internal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderRepository extends JpaRepository<Order, UUID> {

    Optional<Order> findByOrderId(String orderId);

    default List<Order> findAllNewestFirst() {
        return findAll(Sort.by(Sort.Direction.DESC, "completedAt"));
    }
}
