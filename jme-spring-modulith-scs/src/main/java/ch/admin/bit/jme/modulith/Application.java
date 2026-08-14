package ch.admin.bit.jme.modulith;

import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.modulith.Modulithic;

/**
 * The application modules of this service live in the direct sub-packages of this class' package:
 * {@code order}, {@code inventory}, {@code notification} and {@code messaging}. {@link Modulithic}
 * makes Spring Modulith aware of that arrangement at runtime; {@code ModularityTests} verifies it at
 * build time.
 * <p>
 * The service both consumes and produces {@link JmeOrderCreatedEvent}: it consumes it as the external
 * event that drives the example, and produces it from the demo endpoint that stands in for the
 * external system which would publish it in a real deployment.
 */
@SpringBootApplication
@Modulithic
@JeapMessageConsumerContract(JmeOrderCreatedEvent.TypeRef.class)
@JeapMessageProducerContract(JmeOrderCreatedEvent.TypeRef.class)
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
