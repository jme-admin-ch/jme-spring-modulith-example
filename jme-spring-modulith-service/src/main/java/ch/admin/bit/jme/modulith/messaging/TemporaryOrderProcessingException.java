package ch.admin.bit.jme.modulith.messaging;

import ch.admin.bit.jeap.messaging.avro.errorevent.MessageHandlerExceptionInformation;
import lombok.Getter;

/**
 * A <em>temporary</em> processing failure: something that is expected to succeed if tried again later,
 * such as a database that is briefly unavailable.
 * <p>
 * By implementing {@link MessageHandlerExceptionInformation} the exception tells the jEAP error handler
 * how to report the failure. A {@code TEMPORARY} error is resent by the error handling service
 * according to its resending strategy, and only escalated to a permanent error once the configured
 * number of retries is exhausted.
 */
@Getter
class TemporaryOrderProcessingException extends RuntimeException implements MessageHandlerExceptionInformation {

    private static final String ERROR_CODE = "ORDER_PROCESSING_TEMPORARILY_FAILED";

    private final String errorCode = ERROR_CODE;
    private final Temporality temporality = Temporality.TEMPORARY;
    private final String description;
    private final String stackTraceAsString = null;

    TemporaryOrderProcessingException(String orderId) {
        super("Order " + orderId + " could not be processed at the moment");
        this.description = "Simulated temporary failure, triggered by the order type "
                + OrderTypes.FAIL_TEMPORARY;
    }
}
