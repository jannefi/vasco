from pathlib import Path
import re, py_compile

fp = Path("vasco/cli_pipeline.py")
src = fp.read_text(encoding="utf-8")

# 1) Insert helper (only once)
if "_cds_courtesy_pause" not in src:
    helper = """
import os as _os
import time as _time

def _cds_courtesy_pause():
    try:
        pause_sec = float(_os.getenv('VASCO_CDS_PAUSE_SECONDS', '8'))
    except Exception:
        pause_sec = 8.0
    if pause_sec > 0:
        try:
            _time.sleep(pause_sec)
        except Exception:
            pass
"""
    # Place helper after initial import block (first blank line after imports)
    m = re.search(
        r"^(?:from\\s+\\S+\\s+import\\s+\\S+|import\\s+\\S+(?:\\s+as\\s+\\S+)?)(?:.*\\n)+?\\n",
        src,
        flags=re.M,
    )
    insert_at = m.end() if m else 0
    src = src[:insert_at] + helper + src[insert_at:]

# 2) Replace any legacy pause blocks with a call to the helper
# a) Former fixed sleep
src = src.replace("time.sleep(45.0)", "_cds_courtesy_pause()")

# b) Inline pause try-block variants
pause_pattern = re.compile(
    r"""
    try:\s*
        import\ s*,\s*time\s*      # 'import os, time' tolerant spacing
        .*?                        # lines
        time\.sleep\(.*?\)\s*      # sleep call
    except\ Exception:\s*          # except block
        pass
    """,
    re.S | re.X,
)
src = pause_pattern.sub("_cds_courtesy_pause()", src)

# c) Guard any remaining direct time.sleep (rare; comment out original)
src = re.sub(
    r"(\s)time\.sleep\(", r"\1_cds_courtesy_pause(); # replaced\n\1# time.sleep(", src
)

fp.write_text(src, encoding="utf-8")

# Syntax validation
py_compile.compile(str(fp), doraise=True)
print("[OK] Applied _cds_courtesy_pause helper and validated syntax.")
