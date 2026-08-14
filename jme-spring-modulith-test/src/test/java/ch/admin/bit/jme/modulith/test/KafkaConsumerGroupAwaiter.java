package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jeap.jme.test.BootServiceIntegrationTestBase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.common.config.SaslConfigs;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The spawned services connect to whichever Kafka listener matches
 * {@code BootServiceIntegrationTestBase.TestProfileResolver.isCI()} (see each service's
 * application-local.yml/application-ci.yml), so this must use that exact check rather than a
 * separately maintained one.
 */
@Slf4j
final class KafkaConsumerGroupAwaiter {

    private KafkaConsumerGroupAwaiter() {
    }

    static void waitForAssignment(String groupId, String topic) {
        Map<String, Object> adminClientConfig = BootServiceIntegrationTestBase.TestProfileResolver.isCI()
                ? Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:29092")
                : Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT",
                        SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512",
                        SaslConfigs.SASL_JAAS_CONFIG,
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"user-secret\";");

        try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                boolean assigned;
                try {
                    ConsumerGroupDescription description = adminClient.describeConsumerGroups(List.of(groupId))
                            .describedGroups().get(groupId).get();
                    assigned = description.members().stream()
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .anyMatch(topicPartition -> topicPartition.topic().equals(topic));
                } catch (Exception e) {
                    log.warn("Failed to describe consumer group {} while waiting for assignment on topic {}", groupId, topic, e);
                    assigned = false;
                }
                assertThat(assigned).as("consumer group %s assigned to topic %s", groupId, topic).isTrue();
            });
        }
    }
}
