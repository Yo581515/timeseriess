import yaml
import os
from jinja2 import Template


def load_config(config_path: str) -> dict:
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    # lets the yaml file reference {{ env_var('SOME_KEY') }} instead of hardcoding secrets/paths
    def env_var(key: str, default=None, required: bool = False):
        val = os.getenv(key, default)
        if (val is None or val == "") and (required or default is None):
            raise KeyError(f"Missing required environment variable: {key}")
        return val

    try:
        with open(config_path, "r") as f:
            # render the yaml as a jinja template first so env_var() calls get substituted,
            # then parse the result as normal yaml
            template = Template(f.read())
            rendered = template.render(env_var=env_var)
            return yaml.safe_load(rendered) or {}
    except Exception as e:
        raise Exception(f"Error reading config file: {e}")
