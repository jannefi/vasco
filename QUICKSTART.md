# QUICKSTART

1) **Install dependencies**
   - macOS (Homebrew):
     ```bash
     brew install sextractor psfex
     ```
   - Python libs (conda-forge strict recommended):
     ```bash
     conda create -n vasco-py310 python=3.10 -y
     conda activate vasco-py310
     conda install -y -c conda-forge astropy numpy matplotlib requests pandas pyarrow
     ```

2) **Unpack and compile**
   ```bash
   unzip vasco_release_*.zip -d vasco
   cd vasco
   chmod +x run.sh
   python -m py_compile vasco/*.py vasco/utils/*.py
   ```

3) **Run a small test**
   ```bash
   ./run.sh --one --ra 150.1145 --dec 2.2050 --size-arcmin 60
   ```

4) **Full tessellation with auto-retry**
   ```bash
   ./run.sh --tess      --center-ra 150.1145 --center-dec 2.2050      --width-arcmin 60 --height-arcmin 60      --retry-after 4
   ```

5) **Review outputs**
   - `RUN_OVERVIEW.md` (counts + first 10 tiles)
   - `RUN_MISSING.json` / `RUN_COUNTS.json` / `RUN_INDEX.json`
   - Per-tile folders under `tiles/` with `final_catalog.ecsv` and QA histogram

6) **If needed: retry missing later**
   ```bash
   ./run.sh --retry-missing data/runs/<your-run>
   ```
