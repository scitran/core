# This implementation as of July 19 2017 has these resource utilizations of the mongodb container:
#   - 2 million entries: 1.50 Gb
#   - 3 million entries: 2.05 Gb
# The entire docker application was given 6 Gb to use, when given the default 2 Gb,
# the process would frequently crash before 1 million entries were downloaded.

import argparse
import csv
import pymongo
import tarfile
import sys
import logging
import datetime

from api.web.request import AccessTypeList
from api import config
from api.handlers.reporthandler import AccessLogReport, ACCESS_LOG_FIELDS

ARG_TO_PARAMS= {
    'l': 'limit',
    's': 'start_date',
    'e': 'end_date',
    'u': 'uid',
    'j': 'subject',
    't': 'access_types'
}

def download_large_csv(params):
    """
    Script to download large csv files to avoid uwsgi worker running out of memory.
    """
    lim = int(params['limit'])
    params['csv'] = "true"
    params['bin'] = "true"
    params['limit'] = "100000"

    csv_file = open('accesslog.csv', 'w+')
    writer = csv.DictWriter(csv_file, ACCESS_LOG_FIELDS)
    writer.writeheader()

    while lim > 0:
        print lim
        params['limit'] = str(min(lim, 100000))
        report = AccessLogReport(params)
        retort = report.build()
        start_date = str(retort[-1]['timestamp'])
        for doc in retort:
            lim = lim - 1
            try:
                writer.writerow(doc)
            except UnicodeEncodeError as e:
                continue
        csv_file.flush()
        params['start_date'] = start_date

            

    csv_file.close()

def format_arg(args):
    return {ARG_TO_PARAMS[arg]: args[arg] for arg in args if args[arg] != None}

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", help="Start date",            type=str)
        parser.add_argument("-e", help="End date",              type=str)
        parser.add_argument("-u", help="User id",               type=str)
        parser.add_argument("-l", help="Limit",                 type=str)
        parser.add_argument("-j", help="subJect",               type=str)
        parser.add_argument("-t", help="list of access Types",  type=str, nargs='+')

        args = vars(parser.parse_args())
        download_large_csv(format_arg(args))
    except Exception as e:
        logging.exception('Unexpected error in log_csv.py')
        sys.exit(1)