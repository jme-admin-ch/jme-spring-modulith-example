package ch.admin.bit.jme.modulith;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

/**
 * The test that makes the module boundaries real. {@code verify()} fails the build when a module
 * reaches into another module's {@code internal} package, when a module depends on a module it did not
 * declare in {@code @ApplicationModule(allowedDependencies = ...)}, or when the modules form a cycle.
 * <p>
 * Without this test the package structure would be a convention; with it, it is a constraint.
 */
class ModularityTests {

    static final ApplicationModules MODULES = ApplicationModules.of(Application.class);

    @Test
    void verifiesModularStructure() {
        MODULES.verify();
    }

    /**
     * Writes C4 and UML component diagrams plus a canvas per module to
     * {@code target/spring-modulith-docs}. The canvases list each module's Spring beans, published
     * events and consumed events, which makes them a decent starting point for the architecture
     * documentation of a real service.
     */
    @Test
    void writesDocumentationSnippets() {
        new Documenter(MODULES)
                .writeDocumentation()
                .writeModuleCanvases();
    }
}
