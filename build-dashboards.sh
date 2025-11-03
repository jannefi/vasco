for d in data/runs/run-*; do
  [ -d "$d/tiles" ] || continue
  if [ ! -f "$d/RUN_DASHBOARD.md" ] || [ "$d/tiles" -nt "$d/RUN_DASHBOARD.md" ]; then
    echo "Building dashboard for $d ..."
    python -m vasco.cli_dashboard build --run-dir "$d" --top-n 5 --max-tiles 50
  else
    echo "Up-to-date: $d"
  fi
done
