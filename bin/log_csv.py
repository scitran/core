# This implementation as of July 19 2017 has these resource utilizations of the mongodb container:
#   - 2 million entries: 1.50 Gb
#   - 3 million entries: 2.05 Gb
# The entire docker application was given 6 Gb to use, when given the default 2 Gb,
# the process would frequently crash before 1 million entries were downloaded.

import argparse
import unicodecsv as csv
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
    entries = int(params['limit'])
    params['csv'] = "true"
    params['bin'] = "true"
    params['limit'] = "100000"

    csv_file = open('accesslog.csv', 'w+')
    writer = csv.DictWriter(csv_file, ACCESS_LOG_FIELDS)
    writer.writeheader()
    unicode_err_count = 0
    while entries > 0:
        print "{} entries left".format(entries)
        params['limit'] = str(min(entries, 100000))
        report = AccessLogReport(params)
        rep = report.build()
        end_date = str(rep[-1]['timestamp'])
        for doc in rep[:-1]:
            entries = entries - 1
            writer.writerow(doc)

        if len(rep) == 1:
            entries = 0
            writer.writerow(rep[0])
        if len(rep) < int(params['limit']) - 1:
            entries = 0
        csv_file.flush()
        params['end_date'] = end_date

            
    print "Encountered unicode errors and skipped {} entries".format(unicode_err_count)
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
