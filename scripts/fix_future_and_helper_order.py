from pathlib import Path
import re, py_compile

fp = Path('vasco/cli_pipeline.py')
src = fp.read_text(encoding='utf-8')

# --- Remove any misplaced helper + its alias imports (wherever they are now) ---
# Remove helper function definition block
helper_block = re.compile(
    r'(?ms)^[ \t]*def[ \t]+_cds_courtesy_pause\s*\([^)]*\):\n(?:[ \t].*\n)+'
)
src = helper_block.sub('', src)

# Remove alias imports if present anywhere
src = re.sub(r'(?m)^[ \t]*import[ \t]+os[ \t]+as[ \t]+_os[ \t]*\n', '', src)
src = re.sub(r'(?m)^[ \t]*import[ \t]+time[ \t]+as[ \t]+_time[ \t]*\n', '', src)

# --- Find insertion point: after module docstring and after all future imports ---
lines = src.splitlines(True)
i = 0

def is_blank_or_comment(line: str) -> bool:
    ls = line.lstrip()
    return (not ls) or ls.startswith('#')

# Skip leading blanks/comments/encoding/shebang
while i < len(lines) and (
    is_blank_or_comment(lines[i]) or
    lines[i].startswith('#!') or
    'coding:' in lines[i] or 'coding=' in lines[i]
):
    i += 1

# If module docstring at top ("""...""" or '''...'''), skip it entirely
if i < len(lines) and lines[i].lstrip().startswith(('"""',"'''")):
    quote = lines[i].lstrip()[:3]
    i += 1
    while i < len(lines):
        if quote in lines[i]:
            i += 1
            break
        i += 1

# Consume all consecutive "from __future__ import ..." lines
while i < len(lines) and lines[i].startswith('from __future__ import'):
    i += 1

# Now insert helper imports + function here
helper_text = (
    "import os as _os\n"
    "import time as _time\n"
    "\n"
    "def _cds_courtesy_pause():\n"
    "    try:\n"
    "        pause_sec = float(_os.getenv('VASCO_CDS_PAUSE_SECONDS', '8'))\n"
    "    except Exception:\n"
    "        pause_sec = 8.0\n"
    "    if pause_sec > 0:\n"
    "        try:\n"
    "            _time.sleep(pause_sec)\n"
    "        except Exception:\n"
    "            pass\n"
    "\n"
)
lines[i:i] = [helper_text]

fixed = ''.join(lines)
fp.write_text(fixed, encoding='utf-8')

# Validate syntax
py_compile.compile(str(fp), doraise=True)
print('[OK] Restored future-import order and reinserted _cds_courtesy_pause correctly.')
