# Documentation

Background documentation for `jme-spring-modulith-example`. The [root README](../README.md) is the
entry point: what the example is, how to build it and how to run through it. These pages go deeper.

| Page                                                                     | Content                                                                                                              |
|--------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| [Architecture](architecture.md)                                          | The application modules, how a message travels through the system, and the two failure paths                         |
| [Configuration](configuration.md)                                        | Every property, port, topic, role and client the example uses, and why                                               |
| [Design: async event error handling](async-event-error-handling-design.md) | Design for the **planned** bridge that escalates failed internal asynchronous events to the jEAP Error Handling Service |

## Related

- [Spring Modulith reference documentation](https://docs.spring.io/spring-modulith/reference/)
- [jeap-messaging](https://jeap-admin-ch.github.io/docs/jeap-messaging/)
- [jeap-error-handling](https://jeap-admin-ch.github.io/docs/jeap-error-handling/)
- [jeap-spring-boot-starters](https://jeap-admin-ch.github.io/docs/jeap-spring-boot-starters/)
