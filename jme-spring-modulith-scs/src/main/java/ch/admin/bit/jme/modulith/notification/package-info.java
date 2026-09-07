/**
 * The {@code notification} application module. It records a notification for every completed order.
 * <p>
 * It listens to the same {@link ch.admin.bit.jme.modulith.order.OrderCompleted} event as the
 * {@code inventory} module. Two listeners on one event mean two independent rows in the
 * {@code event_publication} table, each completed on its own — which is what makes the delivery
 * guarantee of the registry visible.
 */
@org.springframework.modulith.ApplicationModule(
        displayName = "Notification",
        allowedDependencies = "order")
package ch.admin.bit.jme.modulith.notification;
