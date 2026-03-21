-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- observations table
CREATE TABLE IF NOT EXISTS observations (
    time             TIMESTAMPTZ      NOT NULL,

    -- node (sensor hub / buoy)
    node_source      TEXT,
    node_source_id   TEXT             NOT NULL,
    latitude         DOUBLE PRECISION CHECK (latitude  BETWEEN -90  AND 90),
    longitude        DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),

    -- sensor (individual probe)
    sensor_source    TEXT,
    sensor_source_id TEXT             NOT NULL,
    parameter        TEXT             NOT NULL,
    value            DOUBLE PRECISION,
    unit             TEXT,
    quality_codes    INTEGER[],

    PRIMARY KEY (time, node_source_id, sensor_source_id, parameter)
);

-- convert to hypertable partitioned by time
SELECT create_hypertable(
    'observations',
    'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

-- indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_observations_node_time
    ON observations (node_source_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_observations_sensor_time
    ON observations (sensor_source_id, time DESC);

CREATE INDEX IF NOT EXISTS idx_observations_parameter_time
    ON observations (parameter, time DESC);