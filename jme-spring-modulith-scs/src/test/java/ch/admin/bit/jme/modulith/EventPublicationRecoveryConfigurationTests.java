package ch.admin.bit.jme.modulith;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class EventPublicationRecoveryConfigurationTests {

    @Autowired
    Environment environment;

    @Test
    void incompletePublicationsRecoverThroughTheBudgetAwareStarter() {
        assertThat(environment.getProperty(
                "spring.modulith.events.republish-outstanding-events-on-restart", Boolean.class))
                .isFalse();
        assertThat(environment.getProperty("spring.modulith.events.staleness.published"))
                .isEqualTo("2m");
        assertThat(environment.getProperty("spring.modulith.events.staleness.processing"))
                .isEqualTo("2m");
        assertThat(environment.getProperty("spring.modulith.events.staleness.resubmitted"))
                .isEqualTo("2m");
    }
}
