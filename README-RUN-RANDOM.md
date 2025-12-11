
# VASCO run-random rework: download loop + step sweeper

This replaces `run-random.py` with a parameter-driven runner that supports:

- **download_loop** — continuously run step1-download for random tiles until user interrupts
- **steps** — scan `data/tiles/` and run requested steps (2..6) for tiles missing their outputs

## Usage

### 1) Continuous download
```bash
python run-random.py download_loop   --sleep-sec 15   --size-arcmin 30   --survey dss1-red   --pixel-scale 1.7
```
Downloads tiles into `data/tiles/tile-RA...-DEC.../raw/` until you press CTRL+C.

### 2) Run steps across tiles

Run pass1 wherever raw FITS exists but pass1 is missing:
```bash
python run-random.py steps --steps 2
```

Run PSFEx + pass2:
```bash
python run-random.py steps --steps 3
```

Run CDS xmatch (Gaia+PS1), then within5, then summarize:
```bash
export VASCO_CDS_GAIA_TABLE="I/355/gaiadr3"
export VASCO_CDS_PS1_TABLE="II/349/ps1"
python run-random.py steps --steps 4,5,6 --xmatch-backend cds
```

You can chain multiple steps; the runner will skip tiles where a step is already done:
```bash
python run-random.py steps --steps 2,3,4,5,6 --limit 50
```

## Step detection rules
- **2 (pass1)**: runs if `raw/*.fits` exists **and** `pass1.ldac` is **missing**
- **3 (psf+pass2)**: runs if `pass1.ldac` exists **and** `pass2.ldac` is **missing**
- **4 (xmatch)**: runs if `pass2.ldac` exists **and** `xmatch/sex_*_xmatch*.csv` is **missing**
- **5 (within5)**: runs if any `xmatch/*.csv` exists **and** `_within5arcsec.csv` is **missing**
- **6 (summarize)**: runs if `pass2.ldac` exists **and** `RUN_SUMMARY.md` is **missing**

## Notes
- Uses your existing `vasco.cli_pipeline` subcommands under the hood; respects CDS/local backend settings.
- Writes activity logs to `logs/run_random.log`.
- `data/` stays git-ignored; this script is safe to commit.
