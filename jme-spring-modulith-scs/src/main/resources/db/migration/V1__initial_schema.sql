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

-- One table per application module. The modules do not share tables: each owns its own data and the
-- others reach it only through the module's API.

-- order module
CREATE TABLE orders
(
    id           UUID                     NOT NULL PRIMARY KEY,
    order_id     TEXT                     NOT NULL UNIQUE,
    order_type   TEXT                     NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- inventory module
CREATE TABLE stock_reservation
(
    id          UUID                     NOT NULL PRIMARY KEY,
    order_id    TEXT                     NOT NULL UNIQUE,
    order_type  TEXT                     NOT NULL,
    reserved_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- notification module
CREATE TABLE notification
(
    id       UUID                     NOT NULL PRIMARY KEY,
    order_id TEXT                     NOT NULL UNIQUE,
    message  TEXT                     NOT NULL,
    sent_at  TIMESTAMP WITH TIME ZONE NOT NULL
);

-- shipping module: the module whose event processing can fail on purpose, see ShippingManagement.
CREATE TABLE shipment
(
    id             UUID                     NOT NULL PRIMARY KEY,
    order_id       TEXT                     NOT NULL UNIQUE,
    order_type     TEXT                     NOT NULL,
    handed_over_at TIMESTAMP WITH TIME ZONE NOT NULL
);

CREATE TABLE modulith_publication_failure
(
    publication_id      UUID                     NOT NULL,
    completion_attempts INTEGER                  NOT NULL,
    error_event_id      VARCHAR                  NOT NULL,
    escalated_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (publication_id, completion_attempts)
);

CREATE INDEX modulith_publication_failure_escalated_at
    ON modulith_publication_failure (escalated_at);

CREATE SEQUENCE deferred_message_sequence START WITH 1 INCREMENT 1;

CREATE TABLE deferred_message
(
    id                     BIGINT PRIMARY KEY,
    message                BYTEA                    NOT NULL,
    "key"                  BYTEA,
    cluster_name           VARCHAR,
    topic                  VARCHAR                  NOT NULL,
    message_id             VARCHAR                  NOT NULL,
    message_idempotence_id VARCHAR                  NOT NULL,
    message_type_name      VARCHAR                  NOT NULL,
    message_type_version   VARCHAR,
    created                TIMESTAMP WITH TIME ZONE NOT NULL,
    send_immediately       BOOLEAN,
    schedule_after         TIMESTAMP WITH TIME ZONE,
    sent_immediately       TIMESTAMP WITH TIME ZONE,
    sent_scheduled         TIMESTAMP WITH TIME ZONE,
    failed                 TIMESTAMP WITH TIME ZONE,
    fail_reason            VARCHAR,
    resend                 BOOLEAN DEFAULT FALSE,
    trace_id_high          BIGINT,
    trace_id               BIGINT,
    span_id                BIGINT,
    parent_span_id         BIGINT,
    trace_id_string        VARCHAR,
    sampled                BOOLEAN
);

CREATE INDEX deferred_message_created ON deferred_message (created);
CREATE INDEX deferred_message_send_immediately ON deferred_message (send_immediately);
CREATE INDEX deferred_message_schedule_after ON deferred_message (schedule_after);
CREATE INDEX deferred_message_sent_immediately ON deferred_message (sent_immediately);
CREATE INDEX deferred_message_sent_scheduled ON deferred_message (sent_scheduled);
CREATE INDEX deferred_message_failed ON deferred_message (failed);
CREATE INDEX deferred_message_resend ON deferred_message (resend);

CREATE TABLE shedlock
(
    name       VARCHAR(64)  NOT NULL,
    lock_until TIMESTAMP    NOT NULL,
    locked_at  TIMESTAMP    NOT NULL,
    locked_by  VARCHAR(255) NOT NULL,
    PRIMARY KEY (name)
);
