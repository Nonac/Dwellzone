"""Loads and merges YAML configuration files."""

import os
import yaml

_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_DEFAULT_CONFIG = os.path.join(_PROJECT_ROOT, "configs", "default.yaml")

_config = None


def load_config(config_path=None):
    """Loads a YAML config file, merged on top of defaults.

    Args:
        config_path: Path to a YAML file. If None, uses configs/default.yaml.

    Returns:
        A dict of configuration values.
    """
    global _config

    with open(_DEFAULT_CONFIG, "r") as f:
        base = yaml.safe_load(f)

    if config_path and config_path != _DEFAULT_CONFIG:
        with open(config_path, "r") as f:
            override = yaml.safe_load(f) or {}
        base = _deep_merge(base, override)

    _config = base
    return base


def get_config():
    """Returns the currently loaded config, loading defaults if needed.

    Returns:
        A dict of configuration values.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def _deep_merge(base, override):
    """Recursively merges override dict into base dict.

    Args:
        base: Base configuration dict.
        override: Override dict whose values take precedence.

    Returns:
        A new dict with merged values.
    """
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result
