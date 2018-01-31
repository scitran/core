# Print list of rules with invalid algs
import argparse
import logging
import sys
import pytz

from api import config



def shift_timezone(find, replace):

    utc = pytz.timezone('UTC')

    try:
        find_tz     = pytz.timezone(find)
        replace_tz  = pytz.timezone(replace)
    except:
        logging.exception('Unknown timezone.')


    acquisitions = config.db.acquisitions.find({'timezone': find})

    logging.info('Fixing {} acquisitions...'.format(acquisitions.count()))

    sessions_to_update = set()

    for c in acquisitions:

        # Original timestamp saved without timezone
        timestamp = c['timestamp']

        # Instatiate as proper (replace) timestamp
        updated_timestamp = replace_tz.localize(timestamp)

        # Cast updated timestamp to UTC
        utc_timestamp = updated_timestamp.astimezone(utc)

        # Update record to use UTC and new timezone
        config.db.acquisitions.update({'_id': c['_id']}, {'$set': {'timestamp': utc_timestamp, 'timezone': replace}})

        # Add session id to list of sessions to update
        sessions_to_update.add(c['session'])

    # Update sessions to look for min acquisition timestamp now that all timestamps have been updated
    for sid in sessions_to_update:

        # Find earliest acquisition timestamp
        pipeline = [
            {'$match': {'session': sid}},
            {'$group': {'_id': '$session', 'min_timestamp': { '$min': '$timestamp' }}}
        ]

        result = config.db.command('aggregate', 'acquisitions', pipeline=pipeline)['result'][0]
        min_timestamp = result.get('min_timestamp')

        # Update session to use earliest acquisition timestamp
        if min_timestamp:

            logging.info('Updating session with new timestamp...')
            config.db.sessions.update({'_id': sid}, {'$set': {'timestamp': min_timestamp, 'timezone': replace}})





if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("find",     help="Find containers with this timezone.")
        parser.add_argument("replace",  help="Replace timezone of found containers with this timezone.")

        args = parser.parse_args()

        logging.info('Beginning script execution...')

        shift_timezone(args.find, args.replace)

    except Exception as e:
        logging.exception('Script execution failed.')
        sys.exit(1)

    logging.info('Script execution complete.')
    sys.exit(0)


