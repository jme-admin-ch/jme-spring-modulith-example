package ch.admin.bit.jme.modulith.order;

import ch.admin.bit.jeap.security.resource.semanticAuthentication.SemanticApplicationRole;
import ch.admin.bit.jeap.security.resource.token.JeapAuthenticationToken;
import ch.admin.bit.jeap.security.test.resource.JeapAuthenticationTestTokenBuilder;
import ch.admin.bit.jme.modulith.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.authentication;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Shows that the semantic roles actually guard the REST API of the {@code order} module. The tests set
 * the {@link JeapAuthenticationToken} on the request directly instead of presenting a JWT, so no
 * authorization server is needed.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
@AutoConfigureMockMvc
@Import(TestcontainersConfiguration.class)
@ActiveProfiles("test")
class OrderApiSecurityTests {

    private static final SemanticApplicationRole ORDER_READ = role("order", "read");
    private static final SemanticApplicationRole ORDER_WRITE = role("order", "write");
    private static final SemanticApplicationRole UNRELATED = role("unrelated", "read");

    @Autowired
    MockMvc mvc;

    @Test
    void listOrders_withReadRole_isOk() throws Exception {
        mvc.perform(get("/api/orders").with(authentication(tokenWith(ORDER_READ))))
                .andExpect(status().isOk());
    }

    @Test
    void listOrders_withoutReadRole_isForbidden() throws Exception {
        mvc.perform(get("/api/orders").with(authentication(tokenWith(UNRELATED))))
                .andExpect(status().isForbidden());
    }

    // A request without any token is rejected by the resource server's filter chain rather than by
    // method security, so it is asserted against the really running service in jme-spring-modulith-test.

    @Test
    void createOrder_withWriteRole_isCreated() throws Exception {
        mvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"orderId": "4711", "orderType": "STANDARD"}
                                """)
                        .with(authentication(tokenWith(ORDER_WRITE))))
                .andExpect(status().isCreated());
    }

    /**
     * Reading and writing are separate operations of the same semantic resource, so a token that may
     * read orders may not create them.
     */
    @Test
    void createOrder_withOnlyReadRole_isForbidden() throws Exception {
        mvc.perform(post("/api/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"orderId": "4714", "orderType": "STANDARD"}
                                """)
                        .with(authentication(tokenWith(ORDER_READ))))
                .andExpect(status().isForbidden());
    }

    /**
     * Each module owns its own semantic resource, so a token for the {@code order} resource grants
     * nothing on the {@code inventory} one.
     */
    @Test
    void listInventory_withOrderRoleOnly_isForbidden() throws Exception {
        mvc.perform(get("/api/inventory").with(authentication(tokenWith(ORDER_READ))))
                .andExpect(status().isForbidden());
    }

    private static SemanticApplicationRole role(String resource, String operation) {
        return SemanticApplicationRole.builder()
                .system("jme")
                .resource(resource)
                .operation(operation)
                .build();
    }

    private static JeapAuthenticationToken tokenWith(SemanticApplicationRole... roles) {
        return JeapAuthenticationTestTokenBuilder.create().withUserRoles(roles).build();
    }
}
