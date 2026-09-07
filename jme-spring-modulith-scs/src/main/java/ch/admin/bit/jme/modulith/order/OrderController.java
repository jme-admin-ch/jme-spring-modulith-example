package ch.admin.bit.jme.modulith.order;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.List;

/**
 * The REST API of the {@code order} module.
 * <p>
 * Access is authorized with jEAP <em>semantic</em> roles. A semantic role has the shape
 * {@code system_%tenant_@resource_#operation}; setting {@code jeap.security.oauth2.resourceserver.system-name}
 * to {@code jme} activates that role model. Domain-facing modules own semantic resources: reading
 * orders needs {@code jme_@order_#read}, creating them needs {@code jme_@order_#write}.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
class OrderController {

    private final OrderManagement orderManagement;

    @GetMapping
    @PreAuthorize("hasRole('order', 'read')")
    List<OrderSummary> listOrders() {
        return orderManagement.findAll();
    }

    @GetMapping("/{orderId}")
    @PreAuthorize("hasRole('order', 'read')")
    OrderSummary getOrder(@PathVariable String orderId) {
        return orderManagement.findByOrderId(orderId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Order with id '" + orderId + "' not found"));
    }

    /**
     * Registers an order directly, without going through Kafka. The asynchronous fan-out to the
     * {@code inventory}, {@code notification} and {@code shipping} modules is the same either way.
     */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @PreAuthorize("hasRole('order', 'write')")
    OrderSummary createOrder(@RequestBody @Valid CreateOrderRequest request) {
        return orderManagement.registerOrder(request.orderId(), request.orderType());
    }

    record CreateOrderRequest(@NotBlank String orderId, @NotBlank String orderType) {
    }
}
