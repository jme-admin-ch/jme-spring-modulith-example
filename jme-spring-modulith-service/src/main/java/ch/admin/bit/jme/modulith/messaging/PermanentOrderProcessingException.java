package ch.admin.bit.jme.modulith.messaging;

import ch.admin.bit.jeap.messaging.avro.errorevent.MessageHandlerExceptionInformation;
import lombok.Getter;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * A <em>permanent</em> processing failure: retrying would fail again, because the message itself is
 * the problem.
 * <p>
 * A {@code PERMANENT} error is not resent automatically by the error handling service. It waits there
 * until somebody looks at it and either resends it deliberately or deletes it — the roles
 * {@code jme_@error_#retry} and {@code jme_@error_#delete} exist for exactly that.
 */
@Getter
class PermanentOrderProcessingException extends RuntimeException implements MessageHandlerExceptionInformation {

    private static final String ERROR_CODE = "ORDER_PROCESSING_FAILED";

    private final String errorCode = ERROR_CODE;
    private final Temporality temporality = Temporality.PERMANENT;
    private final String description;

    PermanentOrderProcessingException(String orderId) {
        super("Order " + orderId + " cannot be processed");
        this.description = "Simulated permanent failure, triggered by the order type "
                + OrderTypes.FAIL_PERMANENT;
    }

    @Override
    public String getStackTraceAsString() {
        StringWriter stringWriter = new StringWriter();
        printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }
}
