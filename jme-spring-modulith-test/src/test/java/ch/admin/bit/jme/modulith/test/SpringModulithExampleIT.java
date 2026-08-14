package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test of the happy path and of the semantic role authorization, against the three services
 * started as Maven subprocesses on top of the infrastructure from {@code docker/docker-compose.yml}.
 * <p>
 * The chain under test: an external {@code JmeOrderCreatedEvent} on Kafka is consumed by the
 * {@code messaging} module, which registers an order in the {@code order} module, which publishes an
 * internal {@code OrderCompleted} event, which the {@code inventory} and {@code notification} modules
 * pick up asynchronously through the Spring Modulith event publication registry.
 */
class SpringModulithExampleIT extends SpringModulithExampleITBase {

    @BeforeAll
    static void startServices() throws Exception {
        startAllServices();
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-scs",
                JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC);
    }

    @Test
    void consumedKafkaEventReachesEveryApplicationModule() {

        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        publishOrderCreatedEvent(token, orderId, "STANDARD");

        // The order module registers the order as soon as the event has been consumed...
        await().untilAsserted(() ->
                assertThat(orderIdsOf("/api/orders", token)).contains(orderId));

        // ...and the two @ApplicationModuleListener methods run afterwards, asynchronously and each in
        // its own transaction.
        await().untilAsserted(() ->
                assertThat(orderIdsOf("/api/inventory", token)).contains(orderId));
        await().untilAsserted(() ->
                assertThat(orderIdsOf("/api/notifications", token)).contains(orderId));
    }

    @Test
    void restApiRejectsARequestWithoutAToken() {
        given().baseUri(SCS_BASE_URL)
                .when().get("/api/orders")
                .then().statusCode(401);
    }

    @Test
    void restApiRejectsATokenWithoutTheRequiredSemanticRole() {
        given().baseUri(SCS_BASE_URL)
                .auth().oauth2(accessTokenWithoutRoles())
                .when().get("/api/orders")
                .then().statusCode(403);
    }

    @Test
    void restApiAcceptsATokenWithTheRequiredSemanticRole() {
        get(accessToken(), "/api/orders").then().statusCode(200);
    }

    private List<String> orderIdsOf(String path, String token) {
        return get(token, path).then().statusCode(200).extract().jsonPath().getList("orderId");
    }
}
