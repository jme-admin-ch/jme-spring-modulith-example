package ch.admin.bit.jme.modulith.shipping;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The REST API of the {@code shipping} module, authorized with the semantic role
 * {@code jme_@shipping_#read}.
 */
@RestController
@RequestMapping("/api/shipments")
@RequiredArgsConstructor
class ShippingController {

    private final ShippingManagement shippingManagement;
    private final JdbcClient jdbcClient;

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

    /**
     * Exposes the durable state of one shipping publication so the retry/discard lifecycle can be
     * verified through the deployed service rather than by connecting to its database.
     */
    @GetMapping("/publications/{publicationId}")
    @PreAuthorize("hasRole('shipping', 'read')")
    PublicationSummary publication(@PathVariable UUID publicationId) {
        return jdbcClient.sql("""
                        SELECT id, status, completion_attempts
                          FROM event_publication
                         WHERE id = :publicationId
                           AND listener_id LIKE '%ShippingManagement.on%'
                        """)
                .param("publicationId", publicationId)
                .query(PublicationSummary.class)
                .optional()
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    record PublicationSummary(UUID id, String status, int completionAttempts) {
    }
}
