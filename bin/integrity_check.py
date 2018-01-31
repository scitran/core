# Print list of rules with invalid algs
import argparse
import logging
import sys

from api import config
from api.dao import APINotFoundException
from api.jobs.gears import get_gear_by_name


# Methods should return true if all integrity checks passed
INTEGRITY_CHECKS = {
    "rule_alg" :        "Confirm alg keys in rules table can be resolved to gear in gear table",
    "session_length" :   "Confirm there are no sessions whose acquisition timestamps span more than 3 hours"
}


def rule_alg():
    errors = False

    for rule in config.db.project_rules.find({}):
        alg = rule.get('alg')

        if not alg:
            errors = True
            logging.warning('Rule {} has no alg.'.format(rule['_id']))

        else:
            try:
                get_gear_by_name(alg)
            except APINotFoundException:
                errors = True
                logging.warning('Rule {} with alg {} does not match any gear in the system'.format(rule['_id'], alg))

    return not errors

def session_length():
    errors = False

    pipeline = [
        {'$match': {'timestamp': {'$ne': None}}},
        {'$group': {'_id': '$session', 'min_timestamp': { '$min': '$timestamp' }, 'max_timestamp': { '$max': '$timestamp' }}},
        {'$project': {'_id': '$_id', 'diff': { '$subtract':  ['$max_timestamp', '$min_timestamp']}}},
        {'$match': {'diff': {'$gt': 10800000}}}
    ]

    results = config.db.command('aggregate', 'acquisitions', pipeline=pipeline)['result']
    if len(results) > 0:
        errors = True
        logging.warning('There are {} sessions that span 3 hours.'.format(len(results)))
        for r in results:
            logging.warning('Session {} spans {} minutes'.format(r['_id'], r['diff']/60000))

    return not errors


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--all",                help="Run all checks", action="store_true")

        for method, desc in INTEGRITY_CHECKS.iteritems():
            parser.add_argument("--"+method, help=desc, action="store_true")

        # get list of method to run:
        args = parser.parse_args()
        if args.all:
            methods = INTEGRITY_CHECKS.keys()
        else:
            methods = [m for m, flag in vars(args).iteritems() if flag]


        errors = False
        for method in methods:
            try:
                logging.info('Running {}...'.format(method))
                passed = globals()[method]()
                if not passed:
                    errors = True
                    logging.warning('{} found integrity issues.'.format(method))
                logging.info('{} complete.'.format(method))
            except:
                logging.exception('Failed to run check {}'.format(method))

        if errors:
            logging.error('One or more checks failed')
            sys.exit(1)
        else:
            logging.info('Checks complete.')
            sys.exit(0)

    except Exception as e:
        logging.exception('Main method failed...')
        sys.exit(1)
