package ch.admin.bit.jme.modulith.shipping.internal;

import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ShipmentRepository extends JpaRepository<Shipment, UUID> {

    boolean existsByOrderId(String orderId);

    default List<Shipment> findAllNewestFirst() {
        return findAll(Sort.by(Sort.Direction.DESC, "handedOverAt"));
    }
}
