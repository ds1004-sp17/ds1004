import csv
import sys

# Example: python extract_columns.py data/yellow_tripdata2015-01.csv

in_fname = sys.argv[1]
out_fname = sys.argv[2]
col_id = int(sys.argv[3])

with open(in_fname, 'r') as inf:
    with open(out_fname, 'w') as outf:
        reader = csv.reader(inf)
        for line in reader:
            outf.write(line[col_id])
            outf.write('\n')
