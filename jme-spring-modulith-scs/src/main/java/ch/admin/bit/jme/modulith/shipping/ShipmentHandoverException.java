package ch.admin.bit.jme.modulith.shipping;

/**
 * Thrown when the shipment cannot be handed over to the carrier. Nothing about this exception is
 * special to Spring Modulith: any exception escaping an {@code @ApplicationModuleListener} marks that
 * listener's event publication as failed.
 */
class ShipmentHandoverException extends RuntimeException {

    ShipmentHandoverException(String orderId, int attempt) {
        super("Handing order %s over to the carrier failed on attempt %d".formatted(orderId, attempt));
    }
}
