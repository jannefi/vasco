from __future__ import annotations
import os
from pathlib import Path
import logging

logger = logging.getLogger("vasco")

# --- Image service preference (global) -----------------------------------------
# Default to STScI (science-safe, original pixel grid). Allow overrides via
# env VASCO_IMAGE_SERVICE or config file .vasco_config.yml {image_service: ...}.

_DEFAULT_SERVICE = "stsci"  # 'stsci' | 'skyview'

_DEF_CFG_NAME = ".vasco_config.yml"


def _read_config_service() -> str | None:
    try:
        # Search repo root and CWD for a project-level config file
        here = Path.cwd()
        for root in [here, *here.parents]:
            cfg = root / _DEF_CFG_NAME
            if cfg.exists():
                try:
                    import yaml  # optional; fall back to naive parse

                    with cfg.open() as f:
                        data = yaml.safe_load(f) or {}
                    val = str(data.get("image_service", "")).strip().lower()
                    if val in {"stsci", "skyview"}:
                        return val
                except Exception:
                    # naive parse: image_service: value
                    try:
                        txt = cfg.read_text()
                        for line in txt.splitlines():
                            if "image_service" in line:
                                val = line.split(":", 1)[1].strip().lower()
                                if val in {"stsci", "skyview"}:
                                    return val
                    except Exception:
                        pass
                break
    except Exception:
        pass
    return None


def get_effective_image_service() -> str:
    env = os.environ.get("VASCO_IMAGE_SERVICE", "").strip().lower()
    if env in {"stsci", "skyview"}:
        return env
    cfg = _read_config_service()
    if cfg in {"stsci", "skyview"}:
        return cfg
    return _DEFAULT_SERVICE


# Optional helper to print the chosen service (for CLI utilities)
def describe_image_service() -> str:
    svc = get_effective_image_service()
    if svc == "stsci":
        return "Using STScI DSS endpoint (original pixel grid)."
    else:
        return (
            "Using SkyView service (resampled output). "
            "For science runs, prefer STScI (set VASCO_IMAGE_SERVICE=stsci"
            " or write image_service: stsci to .vasco_config.yml)."
        )


# --- Example integration hooks (call from your fetch functions) ----------------
# These are minimal no-op shims you can call from existing code paths. They do
# not alter signatures; they only compute preferred order and log warnings.


class ImageServiceOrder:
    def __init__(self, primary: str, fallback: str):
        self.primary = primary  # 'stsci' or 'skyview'
        self.fallback = fallback


def get_fetch_order() -> ImageServiceOrder:
    svc = get_effective_image_service()
    if svc == "stsci":
        return ImageServiceOrder("stsci", "skyview")
    return ImageServiceOrder("skyview", "stsci")


def log_service_choice(primary: str):
    if primary == "stsci":
        logger.info("[INFO] %s", describe_image_service())
    else:
        logger.warning("[WARN] %s", describe_image_service())
