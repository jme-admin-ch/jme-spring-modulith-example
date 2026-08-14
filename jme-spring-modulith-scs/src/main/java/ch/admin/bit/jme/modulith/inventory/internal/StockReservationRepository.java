package ch.admin.bit.jme.modulith.inventory.internal;

import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StockReservationRepository extends JpaRepository<StockReservation, UUID> {

    boolean existsByOrderId(String orderId);

    default List<StockReservation> findAllNewestFirst() {
        return findAll(Sort.by(Sort.Direction.DESC, "reservedAt"));
    }
}
