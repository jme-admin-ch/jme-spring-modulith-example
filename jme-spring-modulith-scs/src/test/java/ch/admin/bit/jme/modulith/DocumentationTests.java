package ch.admin.bit.jme.modulith;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;
import org.springframework.modulith.docs.Documenter;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Generates the Spring Modulith documentation as part of the build. Spring Modulith derives it from
 * the same module model the application runs on, so the documentation cannot drift away from the code
 * the way a hand-written architecture chapter does.
 * <p>
 * Everything lands in {@code target/spring-modulith-docs}, where it can be inspected after a build:
 * <ul>
 *     <li>{@code components.puml} — a C4 component diagram of all modules and their relationships</li>
 *     <li>{@code module-&lt;name&gt;.puml} — a diagram per module, showing it and its direct dependencies</li>
 *     <li>{@code module-&lt;name&gt;.adoc} — the application module canvas: the module's Spring beans,
 *         aggregate roots, published events, events it listens to and configuration properties</li>
 *     <li>{@code all-docs.adoc} — an aggregating document linking the diagrams and canvases</li>
 * </ul>
 * The {@code .puml} files are PlantUML sources; render them with any PlantUML tooling to get images.
 */
class DocumentationTests {

    @Test
    void writesDocumentationSnippets() {

        ApplicationModules modules = ApplicationModules.of(Application.class);

        new Documenter(modules)
                .writeDocumentation()
                .writeModuleCanvases();

        // Guard against silently producing nothing: the artifact attached by the build would then be
        // an empty archive rather than a failed build.
        Path outputFolder = Path.of("target", "spring-modulith-docs");
        assertThat(outputFolder).isDirectoryContaining(path -> path.getFileName().toString().equals("all-docs.adoc"));

        modules.forEach(module -> {
            String name = module.getIdentifier().toString();
            assertThat(outputFolder.resolve("module-%s.adoc".formatted(name))).exists();
            assertThat(outputFolder.resolve("module-%s.puml".formatted(name))).exists();
        });

        assertThat(Files.exists(outputFolder.resolve("components.puml"))).isTrue();
    }
}
