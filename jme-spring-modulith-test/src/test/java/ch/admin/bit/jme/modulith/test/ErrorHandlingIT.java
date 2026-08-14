package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jme.messaging.event.order.created.JmeOrderCreatedEvent;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Covers the two failure paths of the Kafka consumer. Both make the consumer throw an exception that
 * implements {@code MessageHandlerExceptionInformation}; the jEAP error handler forwards the failure to
 * {@code jme-messageprocessing-failed}, where jme-spring-modulith-error-scs picks it up and exposes it
 * through its OAuth-protected error query API.
 */
class ErrorHandlingIT extends SpringModulithExampleITBase {

    @BeforeAll
    static void startServices() throws Exception {
        startAllServices();
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-scs",
                JmeOrderCreatedEvent.TypeRef.DEFAULT_TOPIC);
        KafkaConsumerGroupAwaiter.waitForAssignment("jme-spring-modulith-error-scs",
                "jme-messageprocessing-failed");
    }

    @Test
    void permanentFailureShowsUpInTheErrorHandlingService() {

        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        String traceId = publishOrderCreatedEvent(token, orderId, "FAIL_PERMANENT");

        String errorToken = errorHandlingAccessToken();
        await().untilAsserted(() -> assertThat(errorCountForTrace(errorToken, traceId)).isEqualTo(1));

        // The order was never registered: the consumer threw before reaching the order module.
        assertThat(get(token, "/api/orders").then().extract().jsonPath().getList("orderId"))
                .doesNotContain(orderId);
    }

    /**
     * A temporary failure is resent by the error handling service according to its resending strategy
     * (3 retries, 10s apart, see the error-scs application-local.yml). Every attempt fails again here,
     * so the error stays — what this test shows is that the failure is reported and picked up at all.
     */
    @Test
    void temporaryFailureShowsUpInTheErrorHandlingService() {

        String token = accessToken();
        String orderId = UUID.randomUUID().toString();

        String traceId = publishOrderCreatedEvent(token, orderId, "FAIL_TEMPORARY");

        String errorToken = errorHandlingAccessToken();
        await().untilAsserted(() -> assertThat(errorCountForTrace(errorToken, traceId)).isPositive());
    }

    private int errorCountForTrace(String errorToken, String traceId) {
        String filter = """
                {"dateFrom":"","dateTo":"","eventName":"","traceId":"%s","eventId":"",\
                "stacktracePattern":"","states":null,"sortField":"created","sortOrder":"desc","closingReason":""}\
                """.formatted(traceId);
        return given()
                .baseUri(ERROR_SCS_BASE_URL)
                .auth().oauth2(errorToken)
                .contentType(ContentType.JSON)
                .body(filter)
                .when()
                .post("/api/error/?pageIndex=0&pageSize=20")
                .jsonPath().getInt("totalErrorCount");
    }
}
