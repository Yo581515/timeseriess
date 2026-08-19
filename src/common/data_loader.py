import json
import os


def load_json_data(filepath) -> list[dict]:
    """
    Loads data from a local JSON file.
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Data file not found: {filepath}")

    with open(filepath, "r") as f:
        return json.load(f)
