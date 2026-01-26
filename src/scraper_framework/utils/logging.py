from __future__ import annotations
import logging
import logging.config
from pathlib import Path
import yaml


def setup_logging(config_path: str = "configs/logging.yaml") -> None:
    """Setup logging configuration from YAML file."""
    path = Path(config_path)
    if not path.exists():
        # Safe fallback
        logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        return

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logging.config.dictConfig(cfg)


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(name)
