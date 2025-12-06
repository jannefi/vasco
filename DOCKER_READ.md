# Docker help

# build the container

Build the astro-tools image. Use --no-cache for full rebuild.
```bash
docker build -t astro-tools:latest .
```

## interactive mode

This mode allows you to open a shell inside the container and run the tools manually. Local ./data folder is mounted. 
```bash
docker run -it --rm -v "$(pwd)/data":/app/data astro-tools:latest
```

# important env variables

Before running run-random.py, make sure the catalog names for CDS backends are set e.g.
```bash
export VASCO_CDS_GAIA_TABLE="I/350/gaiaedr3"
export VASCO_CDS_PS1_TABLE="II/389/ps1_dr2"
```
Note that the env variables are not persistent. If you exit the container, you need to export them again before next run.

## command mode

example: run stilts command on a local file in data folder
```bash
docker run --rm -v "$(pwd)/data":/app/data astro-tools:latest stilts tcopy in=/data/my_catalog.fits out=/data/my_catalog_copy.fits
```
