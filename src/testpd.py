import os
import csv
import glob

# File containing the file name and number of columns
fact_file = '/home/kotname/Documents/Diplom/Code/DatabaseMapping/out/DoopProgramAnalysis/facts_nr_cols1.txt'
directory = '/home/kotname/Documents/Diplom/Code/DatabaseMapping/out/DoopProgramAnalysis'  # Change this to your directory path
file_pattern = '/home/kotname/Documents/Diplom/Code/DatabaseMapping/out/DoopProgramAnalysis/*/*/merge_db/facts/id*/*.tsv'

# Read the column limits from fact_nr_cols1.txt
column_limits = {}
with open(fact_file, 'r') as f:
    for line in f:
        filename, col_limit = line.strip().split(':')
        column_limits[filename.strip()] = int(col_limit.strip())
# Use glob to find all .tsv files matching the directory pattern
tsv_files = glob.glob(file_pattern, recursive=True)
non_done_files= set()
# Iterate through the found .tsv files and process them
for file_path in tsv_files:
    filename = os.path.basename(file_path)  # Get just the file name

    # If the file is listed in fact_nr_cols1.txt, process it
    if filename in column_limits:
        col_limit = column_limits[filename] + 1  # Keep columns up to 3 + 1 = 4
        if col_limit <= 1:
            non_done_files.add(filename)
            continue
        # Read the TSV file
        with open(file_path, 'r') as infile:
            reader = csv.reader(infile, delimiter='\t')
            rows = [row[:col_limit] for row in reader]  # Truncate columns after 'col_limit'

        # Write the modified content back to the file (or create a new one)
        with open(file_path, 'w', newline='') as outfile:
            writer = csv.writer(outfile, delimiter='\t')
            writer.writerows(rows)

        #print(f'Processed file: {file_path}, kept first {col_limit} columns.')
print(non_done_files)