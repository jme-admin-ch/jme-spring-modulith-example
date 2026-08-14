-- shipping module: the module whose event processing can fail on purpose, see ShippingManagement.
CREATE TABLE shipment
(
    id             UUID                     NOT NULL PRIMARY KEY,
    order_id       TEXT                     NOT NULL UNIQUE,
    order_type     TEXT                     NOT NULL,
    handed_over_at TIMESTAMP WITH TIME ZONE NOT NULL
);
