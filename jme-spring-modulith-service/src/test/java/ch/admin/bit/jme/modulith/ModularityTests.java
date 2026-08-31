package ch.admin.bit.jme.modulith;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

/**
 * The test that makes the module boundaries real. {@code verify()} fails the build when a module
 * reaches into another module's {@code internal} package, when a module depends on a module it did not
 * declare in {@code @ApplicationModule(allowedDependencies = ...)}, or when the modules form a cycle.
 * <p>
 * Without this test the package structure would be a convention; with it, it is a constraint.
 *
 * @see DocumentationTests
 */
class ModularityTests {

    @Test
    void verifiesModularStructure() {
        ApplicationModules.of(Application.class).verify();
    }
}
