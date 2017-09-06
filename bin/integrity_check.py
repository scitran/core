# Print list of rules with invalid algs
import argparse
import logging
import sys

from api import config
from api.dao import APINotFoundException
from api.jobs.gears import get_gear_by_name


# Methods should return true if all integrity checks passed
INTEGRITY_CHECKS = {
    "test_one": "Run test one",
    "test_two": "Run test two",
    "check_rule_alg": "Confirm alg keys in rules table can be resolved to gear in gear table"
}


def test_one():
    logging.warning('ran test 1')
    return False


def test_two():
    logging.warning('ran test 2')
    return True


def check_rule_alg():
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

    except Exception as e:
        logging.exception('Main method failed...')
        sys.exit(1)





