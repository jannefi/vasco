
#!/usr/bin/env python3
# Convert CSV (row_id,ra,dec) -> VOTable for TAP upload.
# row_id -> ASCII bytes (np.dtype('S24')) to force VOTable datatype="char"
# ra/dec -> float64 ("double").
import sys
import numpy as np
from astropy.table import Table, Column

if len(sys.argv) != 3:
    print("Usage: csv_to_votable_positions.py <in.csv> <out.vot>")
    sys.exit(1)

in_csv, out_vot = sys.argv[1], sys.argv[2]
t = Table.read(in_csv, format="csv")

# Encode row_id as ASCII bytes (digits only -> ASCII-safe):
row_id_bytes = np.array([str(x).encode('ascii') for x in t['row_id']], dtype='S24')
out = Table()
out['row_id'] = Column(row_id_bytes)                # -> VOTable FIELD datatype="char"
out['ra']     = Column([float(x) for x in t['ra']],  dtype='float64')
out['dec']    = Column([float(x) for x in t['dec']], dtype='float64')

out.write(out_vot, format="votable", overwrite=True)
print(f"Wrote VOTable as ASCII char: {out_vot}  rows={len(out)}")
