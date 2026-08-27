package ch.admin.bit.jeap.errorhandling;

import ch.admin.bit.jeap.messaging.annotations.JeapMessageConsumerContract;
import ch.admin.bit.jeap.messaging.annotations.JeapMessageProducerContract;
import ch.admin.bit.jeap.modulith.command.discardpublication.DiscardModulithPublicationCommand;
import ch.admin.bit.jeap.modulith.command.retrypublication.RetryModulithPublicationCommand;
import ch.admin.bit.jeap.modulith.event.publicationprocessingfailed.ModulithPublicationProcessingFailedEvent;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
@JeapMessageConsumerContract(value = ModulithPublicationProcessingFailedEvent.TypeRef.class,
        topic = "jme-messageprocessing-failed")
@JeapMessageProducerContract(value = RetryModulithPublicationCommand.TypeRef.class,
        topic = "jme-retry-modulith-publication")
@JeapMessageProducerContract(value = DiscardModulithPublicationCommand.TypeRef.class,
        topic = "jme-discard-modulith-publication")
class ModulithMessagingContracts {
}
