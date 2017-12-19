#!/usr/bin/env python
"""
Remove the `deleted` tag from containers (recursively) or from individual files.
"""
import argparse
import logging
import sys

import bson

from api import config
from api.dao.containerutil import propagate_changes


log = logging.getLogger('scitran.undelete')


def main():
    cont_names = ['projects', 'sessions', 'acquisitions', 'analyses']
    cont_names_str = '|'.join(cont_names)
    ap = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
    ap.add_argument('cont_name', help='container name to undelete {}'.format(cont_names_str))
    ap.add_argument('cont_id', help='container id to undelete (bson.ObjectId)')
    ap.add_argument('filename', nargs='?', help='filename within container (string, optional)')
    args = ap.parse_args(sys.argv[1:] or ['--help'])

    if args.cont_name not in cont_names:
        raise ValueError('Invalid cont_name "{}" (must be one of {})'.format(args.cont_name, cont_names_str))
    if not bson.ObjectId.is_valid(args.cont_id):
        raise ValueError('Invalid cont_id "{}"'.format(args.cont_id))

    args.cont_id = bson.ObjectId(args.cont_id)
    query = {'_id': args.cont_id}
    collection = config.db[args.cont_name]
    container = collection.find_one(query)
    if container is None:
        raise RuntimeError('Cannot find {}/{}'.format(args.cont_name, args.cont_id))

    update = {'$unset': {'deleted': True}}
    if args.filename is None:
        log.info('Removing "deleted" tag from {}/{}...'.format(args.cont_name, args.cont_id))
        collection.update_one(query, update)
        log.info('Removing "deleted" tag from child containers recursively...')
        propagate_changes(args.cont_name, args.cont_id, None, update, include_refs=True)
    else:
        log.info('Removing "deleted" tag from file {}/{}/{}...'.format(args.cont_name, args.cont_id, args.filename))
        for f in container.get('files', []):
            if f['name'] == args.filename:
                del f['deleted']
                break
        else:
            raise RuntimeError('Cannot find {}/{}/{}'.format(args.cont_name, args.cont_id, args.filename))
        collection.update_one(query, {'$set': {'files': container['files']}})
    log.info('Done.')


if __name__ == '__main__':
    try:
        main()
    except (ValueError, RuntimeError) as exc:
        log.error(exc.message)
        sys.exit(1)
