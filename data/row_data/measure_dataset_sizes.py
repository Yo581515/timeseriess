import os
import csv
import re
import json

DATA_DIR = "data/row_data"
OUTPUT_FILE = "data/row_data/dataset_sizes.csv"

pattern = re.compile(r"batch_(\d+)_(\d+)_obs")

rows = []

for filename in sorted(os.listdir(DATA_DIR)):
    if not filename.endswith(".json"):
        continue

    path = os.path.join(DATA_DIR, filename)

    # load JSON data
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # serialize to canonical JSON (removes whitespace differences)
    json_bytes = json.dumps(data, separators=(",", ":")).encode("utf-8")

    data_size_bytes = len(json_bytes)
    data_size_kb = data_size_bytes / 1024
    data_size_mb = data_size_kb / 1024

    match = pattern.search(filename)
    if not match:
        continue

    batch_id = int(match.group(1))
    record_count = int(match.group(2))

    rows.append({
        "dataset": filename.replace(".json", ""),
        "batch_id": batch_id,
        "record_count": record_count,
        "json_data_bytes": data_size_bytes,
        "json_data_kb": round(data_size_kb, 2),
        "json_data_mb": round(data_size_mb, 3),
    })

rows.sort(key=lambda x: x["batch_id"])

with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.DictWriter(
        f,
        fieldnames=[
            "dataset",
            "batch_id",
            "record_count",
            "json_data_bytes",
            "json_data_kb",
            "json_data_mb",
        ],
    )

    writer.writeheader()
    writer.writerows(rows)

print(f"Dataset metadata saved to {OUTPUT_FILE}")