package ch.admin.bit.jme.modulith.order;

import ch.admin.bit.jme.modulith.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.modulith.test.ApplicationModuleTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.modulith.test.Scenario;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test of the {@code order} module alone. {@code @ApplicationModuleTest} bootstraps only the beans of
 * this module, so a hidden dependency on another module would show up here as a missing bean rather
 * than passing unnoticed.
 */
@ApplicationModuleTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OrderIntegrationTests {

    @Autowired
    OrderManagement orderManagement;

    @Test
    void publishesOrderCompletedOnRegistration(Scenario scenario) {
        scenario.stimulate(() -> orderManagement.registerOrder("4711", "STANDARD"))
                .andWaitForEventOfType(OrderCompleted.class)
                .matching(event -> "4711".equals(event.orderId()))
                .toArriveAndVerify(event -> assertThat(event.orderType()).isEqualTo("STANDARD"));
    }

    @Test
    void registeringTheSameOrderTwiceIsIdempotent() {

        OrderSummary first = orderManagement.registerOrder("4712", "STANDARD");
        OrderSummary second = orderManagement.registerOrder("4712", "EXPRESS");

        // The second call returns the order as it was registered the first time and does not publish a
        // second event: Kafka delivers at least once, so the consumer has to tolerate a redelivery.
        // (Only the order id and type are compared: the first summary carries the in-memory timestamp
        // with nanosecond precision, the second one the microsecond precision PostgreSQL stores.)
        assertThat(second.orderId()).isEqualTo(first.orderId());
        assertThat(second.orderType()).isEqualTo("STANDARD");
        assertThat(orderManagement.findAll()).filteredOn(order -> "4712".equals(order.orderId())).hasSize(1);
    }
}
