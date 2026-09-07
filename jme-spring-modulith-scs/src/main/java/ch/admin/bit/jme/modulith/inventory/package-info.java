/**
 * The {@code inventory} application module. It reserves stock for completed orders.
 * <p>
 * It is integrated with the {@code order} module purely through the
 * {@link ch.admin.bit.jme.modulith.order.OrderCompleted} event — it never calls into {@code order} and
 * {@code order} never calls into it. The declared dependency on {@code order} exists only so this
 * module may refer to the event type itself.
 */
@org.springframework.modulith.ApplicationModule(
        displayName = "Inventory",
        allowedDependencies = "order")
package ch.admin.bit.jme.modulith.inventory;
