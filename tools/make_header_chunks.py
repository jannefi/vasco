#!/usr/bin/env python3
import csv, os, sys, math

in_csv   = os.environ.get("UPLOAD", "./work/survivors_supercosmos_upload.csv")
out_dir  = os.environ.get("CHUNK_DIR", "./work/scos_chunks")
size     = int(os.environ.get("CHUNK_SIZE", "5000"))   # adjust to 2000 or 1000 if needed
add_num  = os.environ.get("ADD_NUMBER", "0") == "1"    # if 1, derive 'number' from row_id

os.makedirs(out_dir, exist_ok=True)
with open(in_csv, newline="") as f:
    r = csv.reader(f)
    header = next(r)
    # Ensure expected columns exist
    cols = {name:i for i,name in enumerate(header)}
    for need in ("row_id","ra","dec"):
        if need not in cols:
            sys.exit(f"[ERR] missing column '{need}' in {in_csv}")
    # Extend header if adding 'number'
    if add_num and "number" not in cols:
        header = ["number"] + header
    rows = list(r)

total = len(rows)
digits = max(5, int(math.log10(max(1,total))) + 1)  # zero-pad width
def chunk_iter(seq, n):
    for i in range(0, len(seq), n):
        yield i//n, seq[i:i+n]

for idx, chunk in chunk_iter(rows, size):
    out_path = os.path.join(out_dir, f"chunk_{idx:0{digits}d}.csv")
    with open(out_path, "w", newline="") as out:
        w = csv.writer(out)
        w.writerow(header)
        for row in chunk:
            if add_num and "number" not in cols:
                # row_id is tile_id:NUMBER  -> derive NUMBER
                row_id = row[cols["row_id"]]
                number = row_id.split(":")[-1]
                w.writerow([number] + row)
            else:
                w.writerow(row)
print(f"[OK] wrote chunks to {out_dir} (count={(total + size - 1)//size}, size={size})")

