package ch.admin.bit.jme.modulith.shipping;

import ch.admin.bit.jme.modulith.order.OrderCompleted;
import ch.admin.bit.jme.modulith.shipping.internal.Shipment;
import ch.admin.bit.jme.modulith.shipping.internal.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.modulith.events.ApplicationModuleListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class ShippingManagement {

    /**
     * An order of this type makes handing the shipment over to the carrier fail, so that the failure
     * handling of an internal asynchronous event can be demonstrated. Every other order type is
     * shipped normally.
     */
    public static final String FAIL_ASYNC = "FAIL_ASYNC";

    private final ShipmentRepository shipments;

    /**
     * Counts how often the listener has been invoked per order. The counter deliberately lives in
     * memory rather than in the database: the listener runs in its own transaction, which is rolled
     * back when it throws, so anything it had written would be gone. It makes the retries observable —
     * see {@code GET /api/shipments/attempts}.
     */
    private final Map<String, AtomicInteger> attemptsByOrderId = new ConcurrentHashMap<>();

    /**
     * Hands the order over to the carrier — and fails while doing so for {@link #FAIL_ASYNC} orders.
     * <p>
     * When this method throws, Spring Modulith marks the event publication of <em>this listener</em>
     * {@code FAILED}. The publications of the {@code inventory} and {@code notification} listeners of
     * the same event are unaffected and complete normally: each listener has its own row in
     * {@code event_publication} and its own transaction.
     * <p>
     * A failed publication is retried by
     * {@link ch.admin.bit.jme.modulith.FailedEventPublicationResubmitter}, which is where the retry
     * budget is configured.
     */
    @ApplicationModuleListener
    void on(OrderCompleted event) {

        int attempt = attemptsByOrderId
                .computeIfAbsent(event.orderId(), orderId -> new AtomicInteger())
                .incrementAndGet();

        if (FAIL_ASYNC.equals(event.orderType())) {
            log.warn("Handing order {} over to the carrier failed (attempt {})", event.orderId(), attempt);
            throw new ShipmentHandoverException(event.orderId(), attempt);
        }

        if (shipments.existsByOrderId(event.orderId())) {
            log.info("Order {} has already been handed over to the carrier, skipping", event.orderId());
            return;
        }

        shipments.save(new Shipment(event.orderId(), event.orderType()));
        log.info("Handed order {} over to the carrier (attempt {})", event.orderId(), attempt);
    }

    @Transactional(readOnly = true)
    public List<ShipmentSummary> findAll() {
        return shipments.findAllNewestFirst().stream()
                .map(shipment -> new ShipmentSummary(shipment.getOrderId(), shipment.getOrderType(),
                        shipment.getHandedOverAt()))
                .toList();
    }

    /**
     * How often the listener has been invoked per order, initial attempt plus every retry.
     */
    public Map<String, Integer> attemptsByOrderId() {
        return attemptsByOrderId.entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }
}
