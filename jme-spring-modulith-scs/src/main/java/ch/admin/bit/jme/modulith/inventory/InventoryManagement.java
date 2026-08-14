package ch.admin.bit.jme.modulith.inventory;

import ch.admin.bit.jme.modulith.inventory.internal.StockReservation;
import ch.admin.bit.jme.modulith.inventory.internal.StockReservationRepository;
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
public class InventoryManagement {

    private final StockReservationRepository reservations;

    /**
     * Reacts to an order completed in the {@code order} module.
     * <p>
     * {@code @ApplicationModuleListener} is Spring Modulith's shortcut for
     * {@code @Async @Transactional(REQUIRES_NEW) @TransactionalEventListener}: this method runs on
     * another thread, only after the publishing transaction has committed, and in a transaction of its
     * own. Before the listener is invoked, Spring Modulith writes a row to {@code event_publication}
     * and marks it complete once the method returns normally — so a listener that fails, or a service
     * that dies mid-flight, leaves an incomplete publication that is republished on the next startup.
     * <p>
     * Because a publication can be replayed, the listener has to be idempotent.
     */
    @ApplicationModuleListener
    void on(OrderCompleted event) {

        if (reservations.existsByOrderId(event.orderId())) {
            log.info("Stock for order {} is already reserved, skipping", event.orderId());
            return;
        }

        reservations.save(new StockReservation(event.orderId(), event.orderType()));
        log.info("Reserved stock for order {}", event.orderId());
    }

    @Transactional(readOnly = true)
    public List<ReservationSummary> findAll() {
        return reservations.findAllNewestFirst().stream()
                .map(r -> new ReservationSummary(r.getOrderId(), r.getOrderType(), r.getReservedAt()))
                .toList();
    }
}
