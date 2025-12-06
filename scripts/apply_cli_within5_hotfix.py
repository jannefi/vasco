#!/usr/bin/env python3
"""Apply unit-tolerant within-5" hotfix to vasco/cli_pipeline.py.
- Replaces call-sites: _validate_within_5_arcsec(out_gaia/out_ps1) -> _validate_within_5_arcsec_unit_tolerant(...)
- Replaces the body of _validate_within_5_arcsec(...) with a thin wrapper delegating to the unit-tolerant function.
This script is idempotent.
"""
import re, sys
from pathlib import Path
CLI = Path('vasco/cli_pipeline.py')
if not CLI.exists():
    print('[ERROR] File not found:', CLI)
    sys.exit(2)
text = CLI.read_text(encoding='utf-8')
# 1) Replace call-sites (handle optional spaces)
text = re.sub(r"_validate_within_5_arcsec\s*\(\s*out_gaia\s*\)",
              "_validate_within_5_arcsec_unit_tolerant(out_gaia)", text)
text = re.sub(r"_validate_within_5_arcsec\s*\(\s*out_ps1\s*\)",
              "_validate_within_5_arcsec_unit_tolerant(out_ps1)", text)
# 2) Replace function body with wrapper (multiline-safe)
pat = re.compile(r"^def\s+_validate_within_5_arcsec\s*\([^)]*\):"  # def line
                 r"(?:\n|.)*?"                                   # body lazily
                 r"(?=^\s*def\s|\Z)",                          # until next def or EOF
                 flags=re.M|re.S)
wrapper = (
    "def _validate_within_5_arcsec(xmatch_csv):\n"
    "    from pathlib import Path as _P\n"
    "    return _validate_within_5_arcsec_unit_tolerant(_P(xmatch_csv))\n\n"
)
if pat.search(text):
    text = pat.sub(wrapper, text)
else:
    text = text + "\n\n" + wrapper
CLI.write_text(text, encoding='utf-8')
print('[OK] Patched', CLI)
