package ch.admin.bit.jme.modulith.inventory;

import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The REST API of the {@code inventory} module, authorized with the semantic role
 * {@code jme_@inventory_#read}.
 */
@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
class InventoryController {

    private final InventoryManagement inventoryManagement;

    @GetMapping
    @PreAuthorize("hasRole('inventory', 'read')")
    List<ReservationSummary> listReservations() {
        return inventoryManagement.findAll();
    }
}
