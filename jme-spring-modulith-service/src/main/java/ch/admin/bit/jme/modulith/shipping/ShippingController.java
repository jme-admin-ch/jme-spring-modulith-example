package ch.admin.bit.jme.modulith.shipping;

import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * The REST API of the {@code shipping} module, authorized with the semantic role
 * {@code jme_@shipping_#read}.
 */
@RestController
@RequestMapping("/api/shipments")
@RequiredArgsConstructor
class ShippingController {

    private final ShippingManagement shippingManagement;

    @GetMapping
    @PreAuthorize("hasRole('shipping', 'read')")
    List<ShipmentSummary> listShipments() {
        return shippingManagement.findAll();
    }

    /**
     * How often the listener has run per order. Watching this grow for a {@code FAIL_ASYNC} order is
     * how the resubmission of a failed event publication becomes visible from the outside.
     */
    @GetMapping("/attempts")
    @PreAuthorize("hasRole('shipping', 'read')")
    Map<String, Integer> listAttempts() {
        return shippingManagement.attemptsByOrderId();
    }
}
