import logging
from datetime import datetime, timezone
import json
import json
import logging
from pathlib import Path


def convert_time(data_point: dict, logger: logging.Logger) -> bool:
    """Convert 'time' field to timezone-aware UTC datetime."""
    try:
        time_val = data_point.get("time")

        if not time_val:
            logger.error("[convert_time] Missing 'time'")
            return False

        if isinstance(time_val, str):
            time_val = time_val.replace("Z", "+00:00")
            dt = datetime.fromisoformat(time_val)

        elif isinstance(time_val, datetime):
            dt = time_val

        else:
            logger.error("[convert_time] Invalid 'time' type")
            return False

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        data_point["time"] = dt
        return True

    except Exception as e:
        logger.error(f"[convert_time] Time conversion error: {e}")
        return False


def convert_location(data_point: dict, logger: logging.Logger) -> bool:
    """Convert location to GeoJSON Point {type:'Point', coordinates:[lon,lat]}."""
    try:
        loc = data_point.get("location") or {}
        if not isinstance(loc, dict):
            logger.error("[convert_location] 'location' is not a dict")
            return False

        # already GeoJSON
        if loc.get("type") == "Point" and isinstance(loc.get("coordinates"), list) and len(loc["coordinates"]) == 2:
            return True

        lat = loc.get("latitude", loc.get("lat"))
        lon = loc.get("longitude", loc.get("lon", loc.get("lng")))

        if lat is None or lon is None:
            logger.error("[convert_location] Missing latitude/longitude")
            return False

        data_point["location"] = {"type": "Point", "coordinates": [float(lon), float(lat)]}
        return True

    except Exception as e:
        logger.error(f"[convert_location] Location conversion error: {e}")
        return False


def extract_meta_fields(data_point: dict, logger: logging.Logger) -> bool:
    try:
        source = data_point.get("source")
        source_id = data_point.get("source_id")
        data_point["meta"] = {
            "source": source,
            "source_id": source_id,
        }
        return True

    except Exception as e:
        logger.error(f"[extract_meta_fields] Meta extraction error: {e}")
        return False


def resolve_data(data_point: dict, logger: logging.Logger) -> bool:
    """
    Normalize a doc in-place.
    Returns True if valid, False if it should be dropped.
    """
    if not convert_time(data_point, logger):
        return False
    if not convert_location(data_point, logger):
        return False
    if not extract_meta_fields(data_point, logger):
        return False
    return True




def make_strftime_from_utc(d: dict) -> dict:
    if "time" not in d or d["time"] is None:
        breakpoint()

    dt = d["time"]

    # Enforce UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    d["time"] = dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return d





def load_mongodb_json(file_path: str, logger: logging.Logger) -> list[dict]:
    """
    Load a MongoDB dataset JSON file.

    The file is expected to contain:
      - a list of documents (most common), OR
      - a single document (dict)

    Returns:
      List[dict]: list of MongoDB documents

    Raises:
      FileNotFoundError if file does not exist
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"MongoDB data file not found: {path.resolve()}")

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            logger.info(f"Loaded {len(data)} documents from {path.name}")
            return data

        if isinstance(data, dict):
            logger.info(f"Loaded single document from {path.name}")
            return [data]

        logger.error(
            f"Unexpected JSON root type in {path.name}: {type(data)}"
        )
        return []

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {path.name}: {e}")
        return []

    except Exception as e:
        logger.error(f"Failed to load {path.name}: {e}")
        return []