
import subprocess
from pathlib import Path
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed


N_PARALLEL = int(os.environ.get('N_PARALLEL', '4'))

TILES_ROOT = Path('./data/tiles')
LOGS_DIR = Path('./logs')
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / 'mnras_batch.log'
DONE_MARKER = 'MNRAS_DONE.flag'

def process_tile(tile_dir: Path):
    marker = tile_dir / DONE_MARKER
    log_lines = []
    try:
        if marker.exists():
            msg = f"[SKIP] {tile_dir} already processed."
            print(msg)
            return msg

        log_lines.append(f"\n=== Processing {tile_dir} ===")
        # Step 4: xmatch (CDS backend)
        log_lines.append("  [step4-xmatch]")
        subprocess.run([
            'python', '-m', 'vasco.cli_pipeline', 'step4-xmatch',
            '--workdir', str(tile_dir),
            '--xmatch-backend', 'cds',
            '--xmatch-radius-arcsec', '5'
        ], check=True)
        log_lines.append("    done.")

        # Step 5: filter-within5
        log_lines.append("  [step5-filter-within5]")
        subprocess.run([
            'python', '-m', 'vasco.cli_pipeline', 'step5-filter-within5',
            '--workdir', str(tile_dir)
        ], check=True)
        log_lines.append("    done.")

        # Step 6: summarize
        log_lines.append("  [step6-summarize]")
        subprocess.run([
            'python', '-m', 'vasco.cli_pipeline', 'step6-summarize',
            '--workdir', str(tile_dir)
        ], check=True)
        log_lines.append("    done.")

        # Write marker file last (atomic completion)
        marker.write_text("MNRAS batch completed successfully.\n")
        log_lines.append(f"=== Finished {tile_dir} ===")
        return '\n'.join(log_lines)

    except subprocess.CalledProcessError as e:
        msg = f"[ERROR] {tile_dir}: {e}"
        log_lines.append(msg)
        return '\n'.join(log_lines)
    except Exception as e:
        msg = f"[EXCEPTION] {tile_dir}: {e}"
        log_lines.append(msg)
        return '\n'.join(log_lines)

def main():
    tile_dirs = sorted(TILES_ROOT.glob('tile-RA*-DEC*'))
    print(f"Found {len(tile_dirs)} tiles.")

    with ThreadPoolExecutor(max_workers=N_PARALLEL) as executor, LOG_FILE.open('a') as logf:
        futures = {executor.submit(process_tile, tile_dir): tile_dir for tile_dir in tile_dirs}
        for future in as_completed(futures):
            result = future.result()
            print(result)
            logf.write(result + '\n')
            logf.flush()

if __name__ == "__main__":
    main()
