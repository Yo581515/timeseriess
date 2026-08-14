-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- observations table
CREATE TABLE IF NOT EXISTS observations (
    time             TIMESTAMPTZ      NOT NULL,

    node_source      TEXT,
    node_source_id   TEXT             NOT NULL,
    latitude         DOUBLE PRECISION CHECK (latitude  BETWEEN -90  AND 90),
    longitude        DOUBLE PRECISION CHECK (longitude BETWEEN -180 AND 180),

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


-- ============================================================
-- BULK SYNTHETIC DATA GENERATION
-- Fixed date range: 2011-08-13T00:00:00Z -> 2026-08-13T00:00:00Z (15 years)
-- 5 nodes x 5 parameters x readings every 30 minutes
-- ~ 5 * 5 * (15*365.25*48) = ~6,574,500 rows
-- ============================================================

INSERT INTO observations (
    time, node_source, node_source_id, latitude, longitude,
    sensor_source, sensor_source_id, parameter, value, unit, quality_codes
)
SELECT
    ts AS time,
    'Node ' || node_num AS node_source,
    'sfi_smart_ocean;demo;d' || node_num || ';' || node_num AS node_source_id,
    60.0 + (node_num * 0.01) AS latitude,
    5.0 + (node_num * 0.01) AS longitude,
    param_info.sensor_source,
    'sfi_smart_ocean;demo;d' || node_num || ';' || node_num || ';' || param_info.sensor_code AS sensor_source_id,
    param_info.parameter,
    ROUND(
        (param_info.base_value
         + param_info.amplitude * SIN(EXTRACT(EPOCH FROM ts) / 3600.0 + node_num)
         + (RANDOM() - 0.5) * param_info.noise
        )::numeric, 3
    ) AS value,
    param_info.unit,
    ARRAY[0] AS quality_codes
FROM
    generate_series(1, 5) AS node_num,
    generate_series(
        '2011-08-13T00:00:00Z'::timestamptz,
        '2026-08-13T00:00:00Z'::timestamptz,
        INTERVAL '30 minutes'
    ) AS ts,
    (VALUES
        ('Aanderaa Temperature PROBE',  'AANDERAA_TEMPERATURE',  'sea_water_temperature',            16.0,  4.0, 2.0, 'degrees_C'),
        ('Aanderaa Conductivity PROBE', 'AANDERAA_CONDUCTIVITY', 'sea_water_electrical_conductivity',  4.5,  0.5, 0.3, 'S m-1'),
        ('Aanderaa Salinity PROBE',     'AANDERAA_SALINITY',     'sea_water_salinity',                 29.0, 2.0, 1.0, 'PSU'),
        ('Aanderaa Oxygen PROBE',       'AANDERAA_OXYGEN',       'dissolved_oxygen',                  270.0, 30.0, 15.0, 'umol L-1'),
        ('Aanderaa Turbidity PROBE',    'AANDERAA_TURBIDITY',    'turbidity',                          40.0, 15.0, 8.0, 'NTU')
    ) AS param_info(sensor_source, sensor_code, parameter, base_value, amplitude, noise, unit)
ON CONFLICT DO NOTHING;

DO $$
DECLARE
    row_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM observations;
    RAISE NOTICE 'Seeded observations table with % rows', row_count;
END $$;