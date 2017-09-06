# Print list of rules with invalid algs

from api import config
from api.dao import APINotFoundException
from api.jobs.gears import get_gear_by_name

if __name__ == '__main__':

    for rule in config.db.project_rules.find({}):
        alg = rule.get('alg')

        if not alg:
            print 'Rule {} has no alg.'.format(rule['_id'])

        else:
            try:
                get_gear_by_name(alg)
            except APINotFoundException:
                print 'Rule {} with alg {} does not match any gear in the system'.format(rule['_id'], alg)



