package ch.admin.bit.jme.modulith;

import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import ch.admin.bit.jeap.modulith.command.discardpublication.DiscardModulithPublicationCommand;
import ch.admin.bit.jeap.modulith.command.retrypublication.RetryModulithPublicationCommand;
import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import ch.admin.bit.jme.modulith.messaging.MessagingTopics;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.modulith.Modulithic;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * The application modules of this service live in the direct sub-packages of this class' package:
 * {@code order}, {@code inventory}, {@code notification}, {@code shipping} and {@code messaging}.
 * {@link Modulithic} makes Spring Modulith aware of that arrangement at runtime;
 * {@code ModularityTests} verifies it at build time.
 * <p>
 * Scheduling is enabled for the retry and reconciliation of failed internal asynchronous events and
 * for Spring Modulith's staleness monitor.
 * <p>
 * The service both consumes and produces {@link JmeOrderCreatedEvent}: it consumes it as the external
 * event that drives the example, and produces it from the demo endpoint that stands in for the
 * external system which would publish it in a real deployment.
 */
@SpringBootApplication
@Modulithic
@EnableScheduling
@EntityScan
@EnableJpaRepositories
@JeapMessageConsumerContract(value = JmeOrderCreatedEvent.TypeRef.class, topic = MessagingTopics.ORDER_CREATED)
@JeapMessageProducerContract(value = JmeOrderCreatedEvent.TypeRef.class, topic = MessagingTopics.ORDER_CREATED)
@JeapMessageConsumerContract(value = RetryModulithPublicationCommand.TypeRef.class,
        topic = "jme-retry-modulith-publication")
@JeapMessageConsumerContract(value = DiscardModulithPublicationCommand.TypeRef.class,
        topic = "jme-discard-modulith-publication")
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
