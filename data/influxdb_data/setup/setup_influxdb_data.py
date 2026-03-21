import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

# from databases.influxdb.config import get_influxdb_config
from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

JsonDoc = Dict[str, Any]


def parse_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_time_range_yyyymmdd(events: List[JsonDoc]) -> Tuple[str, str]:
    times: List[datetime] = []

    for e in events:
        dt = parse_time(e.get("time"))
        if dt:
            times.append(dt)

    if not times:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return today, today

    return min(times).strftime("%Y%m%d"), max(times).strftime("%Y%m%d")


def extract_batch_info(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Example:
      batch_1_100_obs.json    -> ("1", "100")
      batch_12_50000_obs.json -> ("12", "50000")
    """
    m = re.search(r"batch_(\d+)_(\d+)_obs\.json$", filename)
    if not m:
        return None, None

    return m.group(1), m.group(2)


def coerce_to_events(obj: Any) -> List[JsonDoc]:
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


def events_to_dataframe(events: List[JsonDoc]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    for e in events:
        t = parse_time(e.get("time"))
        if t is None:
            continue

        node_source = e.get("source")
        node_source_id = e.get("source_id")

        loc = e.get("location") or {}
        latitude = loc.get("latitude")
        longitude = loc.get("longitude")

        observations = e.get("observations") or []
        if not isinstance(observations, list):
            continue

        for o in observations:
            if not isinstance(o, dict):
                continue

            quality_codes = o.get("qualityCodes")

            rows.append(
                {
                    "time": t,
                    "node_source": node_source,
                    "node_source_id": node_source_id,
                    "latitude": latitude,
                    "longitude": longitude,
                    "sensor_source": o.get("source"),
                    "sensor_source_id": o.get("source_id"),
                    "parameter": o.get("parameter"),
                    "value": o.get("value"),
                    "unit": o.get("unit"),
                    "quality_codes": (
                        ",".join(map(str, quality_codes))
                        if isinstance(quality_codes, list)
                        else None
                    ),
                }
            )

    df = pd.DataFrame(rows)

    if not df.empty:
        df["time"] = pd.to_datetime(df["time"], utc=True)

    return df


def export_csv(events: List[JsonDoc], out_path: Union[str, Path]) -> int:
    df = events_to_dataframe(events)

    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(out_path, index=False)
    return len(df)


if __name__ == "__main__":
    config_file_path = "./configs/config-influxdb.yml"
    config = load_config(config_file_path)

    logger = get_logger("setup_influxdb_data.py", config["general"]["log_file"])
    # _influxdb_config = get_influxdb_config(config["database"])

    entity = "data"
    phenomenon = "observations"

    input_folder = Path("data/row_data")
    out_folder = Path(config["general"]["influxdb_data_folder_path"])
    out_folder.mkdir(parents=True, exist_ok=True)

    json_files = sorted(input_folder.glob("batch_*_obs.json"))
    if not json_files:
        raise SystemExit(f"No matching files found in {input_folder.resolve()}")

    for fp in json_files:
        batch_idx, obs_count = extract_batch_info(fp.name)

        if batch_idx and obs_count:
            logger.info(
                f"Processing {fp.name} (batch_idx={batch_idx}, obs_count={obs_count})"
            )
        else:
            logger.warning(
                f"{fp.name}: filename did not match expected batch pattern; using fallback naming."
            )

        try:
            raw_obj = load_json_data(str(fp))
        except Exception as e:
            logger.error(f"Failed reading {fp.name}: {e}")
            continue

        events = coerce_to_events(raw_obj)
        if not events:
            logger.warning(f"{fp.name}: no events found (empty or unexpected JSON root)")
            continue

        start, end = get_time_range_yyyymmdd(events)

        if batch_idx and obs_count:
            out_name = (
                f"{entity}_{phenomenon}_batch_{batch_idx}_{obs_count}_obs_{start}_{end}.csv"
            )
        else:
            out_name = f"{entity}_{phenomenon}_{fp.stem}_{start}_{end}.csv"

        out_path = out_folder / out_name

        try:
            row_count = export_csv(events, out_path)
        except Exception as e:
            logger.error(f"{fp.name}: failed writing csv: {e}")
            continue

        logger.info(f"Saved {row_count} rows -> {out_path.resolve()}")

    logger.info("Done.")