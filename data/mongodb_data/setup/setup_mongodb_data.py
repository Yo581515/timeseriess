import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from src.common.config import load_config
from src.common.logger import get_logger
from src.common.data_loader import load_json_data

from src.databases.mongodb.config import get_mongodb_config
from data.mongodb_data.utils.data_utils import resolve_data, make_strftime_from_utc
from src.validators.json_validator import is_obj_valid_json

JsonDoc = Dict[str, Any]


def save_json(data: Any, out_path: Union[str, Path]) -> None:
    out_fp = Path(out_path)
    out_fp.parent.mkdir(parents=True, exist_ok=True)

    with out_fp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def parse_iso_time(value: Any) -> Optional[datetime]:
    """
    Parse time values that may be:
      - ISO string with Z or timezone offset
      - datetime
    Returns timezone-aware UTC datetime or None.
    """
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

    # keep everything on UTC so times line up with the other db exports
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_time_range_yyyymmdd(docs: List[JsonDoc]) -> Tuple[str, str]:
    # used to name the output file with the actual date range it covers
    times: List[datetime] = []

    for doc in docs:
        dt = parse_iso_time(doc.get("time"))
        if dt:
            times.append(dt)

    if not times:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        return today, today

    return min(times).strftime("%Y%m%d"), max(times).strftime("%Y%m%d")


def extract_batch_info(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Examples:
      batch_1_100_obs.json    -> ("1", "100")
      batch_12_50000_obs.json -> ("12", "50000")
    """
    m = re.search(r"batch_(\d+)_(\d+)_obs\.json$", filename)
    if not m:
        return None, None

    batch_idx = m.group(1)
    obs_count = m.group(2)
    return batch_idx, obs_count


def coerce_to_docs(obj: Any) -> List[JsonDoc]:
    # raw json root can be a list of docs or a single doc, normalize to a list either way
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    if isinstance(obj, dict):
        return [obj]
    return []


if __name__ == "__main__":
    config_file_path = "./configs/config-mgdb.yml"
    config = load_config(config_file_path)

    logger = get_logger("setup_mongodb_data.py", config["general"]["log_file"])
    _mongodb_config = get_mongodb_config(config["database"])

    entity = "data"
    phenomenon = "observations"

    input_folder = Path("data/row_data")
    out_folder = Path("data/mongodb_data")
    out_folder.mkdir(parents=True, exist_ok=True)

    json_files = sorted(input_folder.glob("batch_*_obs.json"))
    if not json_files:
        raise SystemExit(f"No matching files found in {input_folder.resolve()}")

    logger.info(f"Found {len(json_files)} input files in {input_folder.resolve()}")

    for fp in json_files:
        batch_idx, obs_count = extract_batch_info(fp.name)

        file_label = fp.stem
        if batch_idx and obs_count:
            logger.info(
                f"Processing {fp.name} (batch_idx={batch_idx}, obs_count={obs_count})"
            )
        else:
            logger.warning(
                f"Filename did not match expected pattern: {fp.name}. Using fallback naming."
            )

        try:
            raw_obj = load_json_data(str(fp))
        except Exception as e:
            logger.error(f"Failed reading {fp.name}: {e}")
            continue

        raw_docs = coerce_to_docs(raw_obj)
        if not raw_docs:
            logger.warning(f"{fp.name}: no docs found (empty or unexpected JSON root)")
            continue

        resolved_docs: List[JsonDoc] = []
        dropped = 0

        for i, doc in enumerate(raw_docs):
            if not resolve_data(doc, logger):
                logger.warning(f"{fp.name}: dropped doc at index {i} during resolve_data")
                dropped += 1
                continue

            try:
                make_strftime_from_utc(doc)
            except Exception as e:
                logger.error(f"{fp.name}: failed converting time at doc index {i}: {e}")
                dropped += 1
                continue

            if not is_obj_valid_json(doc):
                logger.error(
                    f"{fp.name}: doc at index {i} not JSON-valid after resolving; dropping it."
                )
                dropped += 1
                continue

            resolved_docs.append(doc)

        if not resolved_docs:
            logger.warning(f"{fp.name}: all docs dropped (dropped={dropped})")
            continue

        start, end = get_time_range_yyyymmdd(resolved_docs)

        if batch_idx and obs_count:
            out_name = (
                f"{entity}_{phenomenon}_batch_{batch_idx}_{obs_count}_obs_{start}_{end}.json"
            )
        else:
            out_name = f"{entity}_{phenomenon}_{file_label}_{start}_{end}.json"

        out_path = out_folder / out_name
        save_json(resolved_docs, out_path)

        logger.info(
            f"Saved {len(resolved_docs)} docs (dropped={dropped}) -> {out_path.resolve()}"
        )

    logger.info("Done.")
