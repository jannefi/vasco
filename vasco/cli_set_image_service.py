
"""Set or show the default image service for VASCO downloads.

Usage:
  # Show effective service (env → config → default)
  python -m vasco.cli_set_image_service

  # Persist preference in project config (repo root):
  python -m vasco.cli_set_image_service --image-service stsci
  python -m vasco.cli_set_image_service --image-service skyview

Notes:
- You can override per-run without touching config:
    VASCO_IMAGE_SERVICE=stsci python -m vasco.cli_pipeline one2pass ...
- Downloader reads: env → .vasco_config.yml → built-in default (stsci)
"""
from __future__ import annotations
from pathlib import Path
import argparse

from vasco.downloader_service_pref import (
    get_effective_image_service, describe_image_service, _DEF_CFG_NAME,
)

def _write_config(value: str) -> Path:
    root = Path.cwd()
    cfg = root / _DEF_CFG_NAME
    # Write minimal YAML
    cfg.write_text(f"image_service: {value}")
    return cfg


def main():
    ap = argparse.ArgumentParser(description='Set or show default VASCO image service')
    ap.add_argument('--image-service', choices=['stsci','skyview'], default=None,
                    help='Persist image service preference in .vasco_config.yml')
    args = ap.parse_args()

    if args.image_service is None:
        eff = get_effective_image_service()
        print(describe_image_service())
        print('Effective =', eff)
        return 0

    cfg = _write_config(args.image_service)
    print('Wrote', cfg)
    print('Effective =', get_effective_image_service())
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
