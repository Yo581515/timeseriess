// Runs ONLY on first initialization (empty /data/db)

const DB_NAME = "timeseries_db";
const COLLECTION = "sensor_data";

const dbRef = db.getSiblingDB(DB_NAME);

print(`Creating timeseries collection '${COLLECTION}' in DB '${DB_NAME}'`);

dbRef.createCollection(COLLECTION, {
  timeseries: {
    timeField: "time",
    metaField: "meta",
    granularity: "minutes"
  }
});

dbRef[COLLECTION].createIndex({ "meta.source_id": 1, time: -1 });

print(`Collection '${COLLECTION}' initialized with indexes`);

// ============================================================
// BULK SYNTHETIC DATA GENERATION
// Fixed date range: 2011-08-13T00:00:00Z -> 2026-08-13T00:00:00Z (15 years)
// 5 nodes x readings every 30 minutes
// = 5 * (15*365.25*48) ~= 1,314,900 documents
// each document embeds 5 parameter readings
// ============================================================

const NODES = [1, 2, 3, 4, 5];

const PARAM_INFO = [
  { source: "Aanderaa Temperature PROBE", code: "AANDERAA_TEMPERATURE", parameter: "sea_water_temperature", base: 16.0, amp: 4.0, noise: 2.0, unit: "degrees_C" },
  { source: "Aanderaa Conductivity PROBE", code: "AANDERAA_CONDUCTIVITY", parameter: "sea_water_electrical_conductivity", base: 4.5, amp: 0.5, noise: 0.3, unit: "S m-1" },
  { source: "Aanderaa Salinity PROBE", code: "AANDERAA_SALINITY", parameter: "sea_water_salinity", base: 29.0, amp: 2.0, noise: 1.0, unit: "PSU" },
  { source: "Aanderaa Oxygen PROBE", code: "AANDERAA_OXYGEN", parameter: "dissolved_oxygen", base: 270.0, amp: 30.0, noise: 15.0, unit: "umol L-1" },
  { source: "Aanderaa Turbidity PROBE", code: "AANDERAA_TURBIDITY", parameter: "turbidity", base: 40.0, amp: 15.0, noise: 8.0, unit: "NTU" },
];

const INTERVAL_MINUTES = 30;
const MS_PER_INTERVAL = INTERVAL_MINUTES * 60 * 1000;

// Fixed date range — matches TimescaleDB and InfluxDB seed scripts exactly
const startTime = new Date('2011-08-13T00:00:00Z');
const endTime = new Date('2026-08-13T00:00:00Z');

print(`Generating documents from ${startTime.toISOString()} to ${endTime.toISOString()} ...`);

const BATCH_SIZE = 5000;
let batch = [];
let totalInserted = 0;

for (let ts = startTime.getTime(); ts <= endTime.getTime(); ts += MS_PER_INTERVAL) {
  const time = new Date(ts);
  const hoursSinceEpoch = ts / 1000 / 3600;

  NODES.forEach((nodeNum) => {
    const observations = PARAM_INFO.map((p) => {
      const wave = p.amp * Math.sin(hoursSinceEpoch + nodeNum);
      const noise = (Math.random() - 0.5) * p.noise;
      const value = Math.round((p.base + wave + noise) * 1000) / 1000;

      return {
        source: p.source,
        source_id: `sfi_smart_ocean;demo;d${nodeNum};${nodeNum};${p.code}`,
        parameter: p.parameter,
        value: value,
        unit: p.unit,
        qualityCodes: [0],
      };
    });

    batch.push({
      source: `Node ${nodeNum}`,
      source_id: `sfi_smart_ocean;demo;d${nodeNum};${nodeNum}`,
      location: {
        type: "Point",
        coordinates: [5.0 + nodeNum * 0.01, 60.0 + nodeNum * 0.01],
      },
      time: time,
      observations: observations,
      meta: {
        source: `Node ${nodeNum}`,
        source_id: `sfi_smart_ocean;demo;d${nodeNum};${nodeNum}`,
      },
    });

    if (batch.length >= BATCH_SIZE) {
      dbRef[COLLECTION].insertMany(batch, { ordered: false });
      totalInserted += batch.length;
      batch = [];
      if (totalInserted % 100000 === 0) {
        print(`  ... inserted ${totalInserted} documents so far`);
      }
    }
  });
}

if (batch.length > 0) {
  dbRef[COLLECTION].insertMany(batch, { ordered: false });
  totalInserted += batch.length;
}

print(`Seeded '${COLLECTION}' with ${totalInserted} documents`);