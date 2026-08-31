package ch.admin.bit.jme.modulith.inventory;

import ch.admin.bit.jme.modulith.TestcontainersConfiguration;
import ch.admin.bit.jme.modulith.order.OrderCompleted;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.modulith.test.ApplicationModuleTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.modulith.test.Scenario;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test of the {@code inventory} module alone. The {@code order} module is not part of the context —
 * the test takes its place by publishing the event that module would publish, which is exactly the
 * coupling the two modules have in production.
 */
@ApplicationModuleTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class InventoryIntegrationTests {

    @Autowired
    InventoryManagement inventoryManagement;

    @Test
    void reservesStockOnOrderCompleted(Scenario scenario) {
        scenario.publish(new OrderCompleted("4711", "STANDARD"))
                .andWaitForStateChange(() -> reservationFor("4711"))
                .andVerify(reservation -> assertThat(reservation)
                        .isPresent()
                        .get()
                        .extracting(ReservationSummary::orderType)
                        .isEqualTo("STANDARD"));
    }

    @Test
    void reservingStockTwiceForTheSameOrderIsIdempotent(Scenario scenario) {

        scenario.publish(new OrderCompleted("4713", "STANDARD"))
                .andWaitForStateChange(() -> reservationFor("4713"))
                .andVerify(reservation -> assertThat(reservation).isPresent());

        // An incomplete publication is replayed on restart, so the listener may see the same event
        // twice and must not reserve stock twice.
        scenario.publish(new OrderCompleted("4713", "STANDARD"))
                .andWaitForStateChange(() -> reservationFor("4713"))
                .andVerify(reservation -> assertThat(inventoryManagement.findAll())
                        .filteredOn(summary -> "4713".equals(summary.orderId()))
                        .hasSize(1));
    }

    private java.util.Optional<ReservationSummary> reservationFor(String orderId) {
        return inventoryManagement.findAll().stream()
                .filter(reservation -> orderId.equals(reservation.orderId()))
                .findFirst();
    }
}
