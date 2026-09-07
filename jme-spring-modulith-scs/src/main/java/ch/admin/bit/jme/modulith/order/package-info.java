/**
 * The {@code order} application module. It owns the orders of the system and is the only module that
 * writes them.
 * <p>
 * Its API towards the other modules consists of {@link ch.admin.bit.jme.modulith.order.OrderManagement},
 * {@link ch.admin.bit.jme.modulith.order.OrderSummary} and the
 * {@link ch.admin.bit.jme.modulith.order.OrderCompleted} event. Everything in the nested
 * {@code internal} package is module-internal and inaccessible to the other modules — Spring Modulith
 * enforces that in {@code ModularityTests}.
 * <p>
 * The module declares no allowed dependencies of its own, so it may not depend on any other
 * application module. Integration happens the other way round, through the events it publishes.
 */
@org.springframework.modulith.ApplicationModule(
        displayName = "Order",
        allowedDependencies = {})
package ch.admin.bit.jme.modulith.order;
