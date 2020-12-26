import csv
import json
from pathlib import Path

DATA_FOLDER = Path('/data')

SOURCE_FILE_PATH = DATA_FOLDER.joinpath('in/tables/input.csv')
RESULT_FILE_PATH = DATA_FOLDER.joinpath('out/tables/output.csv')

config = json.load(open(DATA_FOLDER.joinpath('config.json')))
PARAM_PRINT_LINES = config['parameters']['print_rows']

print('Running...')
with open(SOURCE_FILE_PATH, 'r') as input, open(RESULT_FILE_PATH, 'w+', newline='') as out:
    reader = csv.DictReader(input)
    new_columns = reader.fieldnames
    # append row number col
    new_columns.append('row_number')
    writer = csv.DictWriter(out, fieldnames=new_columns, lineterminator='\n', delimiter=',')
    writer.writeheader()
    for index, l in enumerate(reader):
        # print line
        if PARAM_PRINT_LINES:
            print(f'Printing line {index}: {l}')
        # add row number
        l['row_number'] = index
        writer.writerow(l)
