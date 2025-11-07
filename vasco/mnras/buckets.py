from __future__ import annotations
from typing import Dict


def init_buckets() -> Dict[str, int]:
    return {
        'total_after_filters': 0,
        'spikes_rejected': 0,
        'morphology_rejected': 0,
        'matched_ps1_or_gaia': 0,
        'hpm_objects': 0,
        'unidentified': 0,
        # placeholders for paper categories we do not fully implement yet
        'asteroids': 0,
        'variables': 0,
        'supercosmos_artifacts': 0,
    }


def finalize(b: Dict[str, int]) -> Dict[str, int]:
    # ensure counts are present; could add sanity checks here
    return b
