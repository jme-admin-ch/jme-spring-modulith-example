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
