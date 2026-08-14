#!/bin/bash
set -e

echo "Generating synthetic line-protocol data for InfluxDB seed..."

DATA_FILE="/tmp/seed_data.lp"
INTERVAL_MINUTES=30

# Fixed date range — matches TimescaleDB and MongoDB seed scripts exactly
# start_ts = 2011-08-13T00:00:00Z, end_ts = 2026-08-13T00:00:00Z

awk -v interval_min="$INTERVAL_MINUTES" '
BEGIN {
    srand(42)  # fixed seed for reproducibility

    start_ts = 1313193600   # 2011-08-13T00:00:00Z
    end_ts   = 1786579200   # 2026-08-13T00:00:00Z
    step_sec = interval_min * 60

    split("1 2 3 4 5", nodes, " ")

    n_params = 5
    split("sea_water_temperature sea_water_electrical_conductivity sea_water_salinity dissolved_oxygen turbidity", param_name, " ")
    split("AANDERAA_TEMPERATURE AANDERAA_CONDUCTIVITY AANDERAA_SALINITY AANDERAA_OXYGEN AANDERAA_TURBIDITY", param_code, " ")
    split("Aanderaa_Temperature_PROBE Aanderaa_Conductivity_PROBE Aanderaa_Salinity_PROBE Aanderaa_Oxygen_PROBE Aanderaa_Turbidity_PROBE", param_source, " ")
    split("16.0 4.5 29.0 270.0 40.0", base, " ")
    split("4.0 0.5 2.0 30.0 15.0", amp, " ")
    split("2.0 0.3 1.0 15.0 8.0", noise, " ")
    split("degrees_C S_m-1 PSU umol_L-1 NTU", unit, " ")

    count = 0
    for (ts = start_ts; ts <= end_ts; ts += step_sec) {
        hours = ts / 3600
        for (ni = 1; ni <= 5; ni++) {
            node = nodes[ni]
            lat = 60.0 + node * 0.01
            lon = 5.0 + node * 0.01
            for (pi = 1; pi <= n_params; pi++) {
                wave = amp[pi] * sin(hours + node)
                jitter = (rand() - 0.5) * noise[pi]
                value = base[pi] + wave + jitter

                printf "ocean_observations,node_source=Node_%s,node_source_id=sfi_smart_ocean;demo;d%s;%s,sensor_source=%s,sensor_source_id=sfi_smart_ocean;demo;d%s;%s;%s,parameter=%s,unit=%s value=%.3f,latitude=%.5f,longitude=%.5f,quality_codes=\"[0]\" %d\n",
                    node, node, node, param_source[pi], node, node, param_code[pi], param_name[pi], unit[pi], value, lat, lon, ts

                count++
            }
        }
    }
    print "Generated " count " points" > "/dev/stderr"
}
' > "$DATA_FILE"

echo "Line protocol file generated: $(wc -l < $DATA_FILE) lines"
echo "Writing to InfluxDB bucket ${DOCKER_INFLUXDB_INIT_BUCKET}..."

influx write \
    --bucket "${DOCKER_INFLUXDB_INIT_BUCKET}" \
    --org "${DOCKER_INFLUXDB_INIT_ORG}" \
    --token "${DOCKER_INFLUXDB_INIT_ADMIN_TOKEN}" \
    --precision s \
    -f "$DATA_FILE"

echo "Seed complete."
rm -f "$DATA_FILE"