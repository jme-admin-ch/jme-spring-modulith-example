package ch.admin.bit.jme.modulith;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Runs the tests against a real PostgreSQL, because the Spring Modulith event publication registry and
 * the Flyway migrations of this service are PostgreSQL-specific. {@code @ServiceConnection} points
 * {@code spring.datasource.*} at the container without any further configuration.
 * <p>
 * The image name is written without a registry: the registry images are pulled from is configured in
 * {@code src/test/resources/testcontainers.properties}.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(DockerImageName.parse("postgres:18.4"));
    }
}
