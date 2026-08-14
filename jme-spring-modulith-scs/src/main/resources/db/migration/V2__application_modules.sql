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
