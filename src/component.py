'''
Template Component main class.

'''

import csv
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from kbc.env_handler import KBCEnvHandler

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_PRINT_HELLO = 'print_hello'

# #### Keep for debug
KEY_DEBUG = 'debug'

# list of mandatory parameters => if some is missing, component will fail with readable message on initialization.
MANDATORY_PARS = [KEY_DEBUG]
MANDATORY_IMAGE_PARS = []

APP_VERSION = '0.0.1'


class Component(KBCEnvHandler):

    def __init__(self, debug=False):
        # for easier local project setup
        default_data_dir = Path(__file__).resolve().parent.parent.joinpath('data').as_posix() \
            if not os.environ.get('KBC_DATADIR') else None

        KBCEnvHandler.__init__(self, MANDATORY_PARS, log_level=logging.DEBUG if debug else logging.INFO,
                               data_path=default_data_dir)
        # override debug from config
        if self.cfg_params.get(KEY_DEBUG):
            debug = True
        if debug:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info('Running version %s', APP_VERSION)
        logging.info('Loading configuration...')

        try:
            # validation of mandatory parameters. Produces ValueError
            self.validate_config(MANDATORY_PARS)
            self.validate_image_parameters(MANDATORY_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)
        # ####### EXAMPLE TO REMOVE
        # intialize instance parameteres

        # ####### EXAMPLE TO REMOVE END

    def run(self):
        '''
        Main execution code
        '''

        # ####### ADDED CODE
        last_state = self.get_state_file()
        print(last_state.get("last_update", ''))

        DATA_FOLDER = Path('/data')

        in_table_defs = self.get_input_tables_definitions()
        first_table_def = in_table_defs[0]
        SOURCE_FILE_PATH = first_table_def.full_path
        RESULT_FILE_PATH = os.path.join(self.tables_out_path, 'output.csv')

        config = self.cfg_params  # noqa
        PARAM_PRINT_LINES = config['print_rows']

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

        self.configuration.write_table_manifest(filename=RESULT_FILE_PATH, primary_key=['row_number'], incremental=True)

        now_str = str(datetime.now().date())
        self.write_state_file({"last_update": now_str})
        # ####### ADDED CODE END


"""
        Main entrypoint
"""
if __name__ == "__main__":
    if len(sys.argv) > 1:
        debug_arg = sys.argv[1]
    else:
        debug_arg = False
    try:
        comp = Component(debug_arg)
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(1)
