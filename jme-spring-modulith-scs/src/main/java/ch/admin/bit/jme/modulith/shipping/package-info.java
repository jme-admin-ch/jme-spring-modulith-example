/**
 * The {@code shipping} application module. Like {@code inventory} and {@code notification} it reacts
 * to {@link ch.admin.bit.jme.modulith.order.OrderCompleted}, but it is the module whose processing can
 * <em>fail</em>: it stands for the step that talks to an external carrier, which may be unavailable.
 * <p>
 * It exists to demonstrate what happens when an internal asynchronous event cannot be processed — the
 * Spring Modulith event publication registry marks the publication {@code FAILED} and the resubmission
 * configured by the Modulith error handling starter retries it, while
 * the other two listeners of the same event complete unaffected.
 */
@org.springframework.modulith.ApplicationModule(
        displayName = "Shipping",
        allowedDependencies = "order")
package ch.admin.bit.jme.modulith.shipping;
