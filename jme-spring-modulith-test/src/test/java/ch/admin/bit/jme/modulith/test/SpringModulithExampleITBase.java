package ch.admin.bit.jme.modulith.test;

import ch.admin.bit.jeap.jme.test.BootServiceSpringIntegrationTestBase;
import io.restassured.http.ContentType;
import io.restassured.response.Response;

import static io.restassured.RestAssured.given;

/**
 * Common setup for the integration tests: the URLs of the three services, the access tokens issued by
 * the OAuth mock server, and the calls against the REST API of jme-spring-modulith-scs.
 */
abstract class SpringModulithExampleITBase extends BootServiceSpringIntegrationTestBase {

    static final String AUTH_BASE_URL = "http://localhost:8091/jme-spring-modulith-auth-scs";
    static final String SCS_BASE_URL = "http://localhost:8090/jme-spring-modulith-scs";
    static final String ERROR_SCS_BASE_URL = "http://localhost:8092/error-handling";

    /** Client carrying one semantic role per application module, see the auth-scs application-local.yml. */
    static final String CLIENT_ID = "jme-spring-modulith-client";
    /** Client authenticated the same way but without any of the roles the REST API requires. */
    static final String CLIENT_ID_WITHOUT_ROLES = "jme-spring-modulith-client-without-roles";
    /** Client for the error handling service, whose audience and roles are its own. */
    static final String ERROR_CLIENT_ID = "jme-spring-modulith-error-client";
    static final String CLIENT_SECRET = "secret";

    static void startAllServices() throws Exception {
        startService("jme-spring-modulith-auth-scs", AUTH_BASE_URL);
        startService("jme-spring-modulith-error-scs", ERROR_SCS_BASE_URL);
        startService("jme-spring-modulith-scs", SCS_BASE_URL);
    }

    String accessToken() {
        return fetchAccessToken(AUTH_BASE_URL, CLIENT_ID, CLIENT_SECRET);
    }

    String accessTokenWithoutRoles() {
        return fetchAccessToken(AUTH_BASE_URL, CLIENT_ID_WITHOUT_ROLES, CLIENT_SECRET);
    }

    String errorHandlingAccessToken() {
        return fetchAccessToken(AUTH_BASE_URL, ERROR_CLIENT_ID, CLIENT_SECRET);
    }

    /**
     * Publishes a JmeOrderCreatedEvent to Kafka through the demo endpoint and returns the trace id of
     * the call, which correlates the event with the entry the error handling service may create for it.
     */
    String publishOrderCreatedEvent(String token, String orderId, String orderType) {
        return given()
                .baseUri(SCS_BASE_URL)
                .auth().oauth2(token)
                .queryParam("orderId", orderId)
                .queryParam("orderType", orderType)
                .when()
                .post("/api/demo/orders")
                .then()
                .statusCode(202)
                .extract().jsonPath().getString("traceId");
    }

    Response get(String token, String path) {
        return given()
                .baseUri(SCS_BASE_URL)
                .auth().oauth2(token)
                .accept(ContentType.JSON)
                .when()
                .get(path);
    }
}
