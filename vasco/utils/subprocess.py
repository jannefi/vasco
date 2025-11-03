from __future__ import annotations
import subprocess, logging, os
from typing import Sequence

log = logging.getLogger('vasco')

def run_cmd(cmd: Sequence[str], cwd: str | None = None, timeout: int | None = None):
    tool = os.path.basename(cmd[0])
    out_path = err_path = None
    if cwd:
        out_path = os.path.join(cwd, f"{tool}.out")
        err_path = os.path.join(cwd, f"{tool}.err")
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    if out_path:
        try:
            with open(out_path, 'w') as f: f.write(proc.stdout or '')
            with open(err_path, 'w') as f: f.write(proc.stderr or '')
        except Exception:
            pass
    if proc.returncode != 0:
        if proc.stdout: print(proc.stdout)
        if proc.stderr: print(proc.stderr)
        log.error('Command failed (%s): %s', proc.returncode, ' '.join(cmd))
        raise subprocess.CalledProcessError(proc.returncode, cmd, proc.stdout, proc.stderr)
    return proc
