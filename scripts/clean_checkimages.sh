
#!/bin/bash
# clean_checkimages.sh
# Deletes SExtractor diagnostic images from all tile folders

echo "Searching for resi_pass1.fits, chi_pass1.fits, samp_pass1.fits under ./data/tiles/ ..."

COUNT=$(find ./data/tiles -type f \( -name 'resi_pass1.fits' -o -name 'chi_pass1.fits' -o -name 'samp_pass1.fits' \) | wc -l)
echo "Found $COUNT files to delete."

if [ "$COUNT" -eq 0 ]; then
    echo "No files found. Nothing to do."
    exit 0
fi

# Preview files to be deleted
find ./data/tiles -type f \( -name 'resi_pass1.fits' -o -name 'chi_pass1.fits' -o -name 'samp_pass1.fits' \) -print

# Uncomment the next line to actually delete the files:
# find ./data/tiles -type f \( -name 'resi_pass1.fits' -o -name 'chi_pass1.fits' -o -name 'samp_pass1.fits' \) -delete

echo "To delete these files, uncomment the last line in this script."
echo "Or run the following command directly:"
echo "find ./data/tiles -type f \\( -name 'resi_pass1.fits' -o -name 'chi_pass1.fits' -o -name 'samp_pass1.fits' \\) -delete"

