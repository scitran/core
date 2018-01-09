#!/usr/bin/env python
"""
Undo a container or file deletion action previously performed via the API.

Container undeletion is propagated to all child/referring containers that
were deleted at the same time as the target container (ie. as part of the
same API call).

Undeleting a container which has deleted parent(s) raises an error, unless
`--include-parents` is specified, which also restores all parent containers
(without further propagation) regardless whether they were deleted at the
same time or not.

Undeleting a container without a deleted timestamp doesn't propagate, unless
`--always-propagate` is specified.
"""
import argparse
import logging
import sys

import bson

from api import config
from api.dao.containerutil import pluralize, propagate_changes


log = logging.getLogger('scitran.undelete')
cont_names = ['projects', 'sessions', 'acquisitions', 'analyses', 'collections']
cont_names_str = '|'.join(cont_names)


def main(*argv):
    ap = argparse.ArgumentParser(description=sys.modules[__name__].__doc__)
    ap.add_argument('cont_name', help='container name to undelete ({})'.format(cont_names_str))
    ap.add_argument('cont_id', help='container id to undelete (bson.ObjectId)')
    ap.add_argument('filename', nargs='?', help='filename within container (optional)')
    ap.add_argument('--include-parents', action='store_true', help='restore deleted parent containers')
    ap.add_argument('--always-propagate', action='store_true', help='propagate even without deleted tag')
    args = ap.parse_args(argv or sys.argv[1:] or ['--help'])

    try:
        undelete(args.cont_name, args.cont_id, filename=args.filename,
                 include_parents=args.include_parents,
                 always_propagate=args.always_propagate)
        log.info('Done.')
    except (AssertionError, RuntimeError, ValueError) as exc:
        log.error(exc.message)
        sys.exit(1)


def undelete(cont_name, cont_id, filename=None, include_parents=False, always_propagate=False):
    if cont_name not in cont_names:
        raise ValueError('Invalid cont_name "{}" (must be one of {})'.format(cont_name, cont_names_str))
    if not bson.ObjectId.is_valid(cont_id):
        raise ValueError('Invalid cont_id "{}" (must be parseable ObjectId)'.format(cont_id))

    cont_id = bson.ObjectId(cont_id)
    cont_str = '{}/{}'.format(cont_name, cont_id)
    container = get_container(cont_name, cont_id)
    if container is None:
        raise RuntimeError('Cannot find {}'.format(cont_str))

    if cont_name == 'collections':
        log.warning('Undeleting collections is limited such that any acquisitions or sessions'
                    'will have to be re-added to the collection again. The files and notes of'
                    'the collection are fully restored.')

    unset_deleted = {'$unset': {'deleted': True}}
    for parent_name, parent_id in get_parent_refs(cont_name, cont_id, filename=filename):
        parent_str = '{}/{}'.format(parent_name, parent_id)
        parent = get_container(parent_name, parent_id)
        if 'deleted' in parent:
            assert parent['deleted'] >= container['deleted']
            if not include_parents:
                msg = ('Found parent {}\n'
                       'which was deleted {} {}.\n'
                       'Run undelete against the parent first to restore it with propagation,\n'
                       'or use `--include-parents` to restore parents without propagation.')
                deleted_time = 'at the same time as' if parent['deleted'] == container['deleted'] else 'after'
                raise RuntimeError(msg.format(parent_str, deleted_time, cont_str))
            log.info('Removing "deleted" tag from parent %s...', parent_str)
            config.db[parent_name].update_one({'_id': parent_id}, unset_deleted)

    if filename is None:
        # Undeleting a container (and any children/referrers)
        if 'deleted' in container:
            log.info('Removing "deleted" tag from %s...', cont_str)
            config.db[cont_name].update_one({'_id': cont_id}, unset_deleted)
            propagate_query = {'deleted': container['deleted']}
        elif always_propagate:
            propagate_query = {}
        else:
            log.info('Skipping %s - has no "deleted" tag', cont_str)
            return
        log.info('Removing "deleted" tag from child/referring containers...')
        propagate_changes(cont_name, cont_id, propagate_query, unset_deleted, include_refs=True)

    else:
        # Undeleting a single file
        file_str = '{}/{}'.format(cont_str, filename)
        for f in container.get('files', []):
            if f['name'] == filename:
                if 'deleted' not in f:
                    log.info('Skipping file %s - has no "deleted" tag', file_str)
                    return
                log.info('Removing "deleted" tag from file %s...', file_str)
                del f['deleted']
                config.db[cont_name].update_one({'_id': cont_id}, {'$set': {'files': container['files']}})
                break
        else:
            raise RuntimeError('Cannot find file {}'.format(file_str))


def get_container(cont_name, cont_id):
    return config.db[cont_name].find_one({'_id': cont_id})


def get_parent_refs(cont_name, cont_id, filename=None):
    parent_name, parent_id = None, None

    container = get_container(cont_name, cont_id)
    if filename is not None:
        parent_name, parent_id = cont_name, cont_id
    elif cont_name == 'analyses':
        parent_name, parent_id = pluralize(container['parent']['type']), container['parent']['id']
    elif cont_name == 'acquisitions':
        parent_name, parent_id = 'sessions', container['session']
    elif cont_name == 'sessions':
        parent_name, parent_id = 'projects', container['project']

    if parent_name is None:
        return []
    return [(parent_name, parent_id)] + get_parent_refs(parent_name, parent_id)


if __name__ == '__main__':
    main()
