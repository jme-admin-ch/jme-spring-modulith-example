-- Schema of the Spring Modulith event publication registry, copied verbatim from
-- org/springframework/modulith/events/jdbc/schemas/v2/schema-postgresql.sql in spring-modulith-events-jdbc.
--
-- Spring Modulith can also create this table itself
-- (spring.modulith.events.jdbc.schema-initialization.enabled=true). It is managed with Flyway here
-- because that is how a jEAP service manages its schema, and because the registry holds application
-- state that outlives a restart and therefore deserves a migration history.

CREATE TABLE IF NOT EXISTS event_publication
(
  id                     UUID NOT NULL,
  listener_id            TEXT NOT NULL,
  event_type             TEXT NOT NULL,
  serialized_event       TEXT NOT NULL,
  publication_date       TIMESTAMP WITH TIME ZONE NOT NULL,
  completion_date        TIMESTAMP WITH TIME ZONE,
  status                 TEXT,
  completion_attempts    INT,
  last_resubmission_date TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (id)
);
CREATE INDEX IF NOT EXISTS event_publication_serialized_event_hash_idx ON event_publication USING hash(serialized_event);
CREATE INDEX IF NOT EXISTS event_publication_by_completion_date_idx ON event_publication (completion_date);
