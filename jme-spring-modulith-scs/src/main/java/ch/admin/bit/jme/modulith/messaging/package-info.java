/**
 * The {@code messaging} application module: the adapter between Kafka and the domain modules.
 * <p>
 * It is the only module that knows the Avro types generated from the jme message type registry. The
 * {@code order} module deals in its own vocabulary and would not change if the external contract did —
 * an anti-corruption layer, expressed as a module boundary that Spring Modulith enforces.
 * <p>
 * It also hosts the demo endpoint that publishes {@code JmeOrderCreatedEvent}. In a real deployment
 * that event would come from another system; publishing it here keeps the example runnable with
 * nothing but curl.
 */
@org.springframework.modulith.ApplicationModule(
        displayName = "Messaging",
        allowedDependencies = "order")
package ch.admin.bit.jme.modulith.messaging;
