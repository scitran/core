#!/usr/bin/env python
import argparse
import datetime
import logging
import os
import uuid

import fs.path
import fs.move

from api import config, util

log = logging.getLogger('migrate_storage')
log.setLevel(logging.INFO)

COLLECTIONS_PREFIXES = [('projects', 'files'),
                            ('acquisitions', 'files'),
                            ('analyses', 'files'),
                            ('sessions', 'files'),
                            ('sessions', 'subject.files'),
                            ('collections', 'files')]


class MigrationError(Exception):
    pass


def get_files_by_prefix(document, prefix):
    for key in prefix.split('.'):
        document = document.get(key, {})
    return document


def show_progress(current_index, total_files):
    if current_index % (total_files / 10 + 1) == 0:
        log.info('Processed %s files of total %s files ...' % (current_index, total_files))


def cleanup_empty_folders():
    log.info('Cleanup empty folders')

    for _dirpath, _, _ in os.walk(config.get_item('persistent', 'data_path'), topdown=False):
        if not (os.listdir(_dirpath) or config.get_item('persistent', 'data_path') == _dirpath):
            os.rmdir(_dirpath)


def get_collections_files():
    _files = []

    for collection, prefix in COLLECTIONS_PREFIXES:
        cursor = config.db.get_collection(collection).find({})
        for document in cursor:
            for f in get_files_by_prefix(document, prefix):
                f_dict = {
                    'collection_id': document.get('_id'),
                    'collection': collection,
                    'fileinfo': f,
                    'prefix': prefix
                }
                _files.append(f_dict)

    return _files


def get_gears_files():
    cursor = config.db.get_collection('gears').find({})
    _files = []

    for document in cursor:
        if document.get('exchange', {}).get('git-commit', '') == 'local':
            f_dict = {
                'gear_id': document['_id'],
                'gear_name': document['gear']['name'],
                'exchange': document['exchange']
            }
            _files.append(f_dict)

    return _files


def migrate_collections():
    log.info('Migrate collection files...')

    _files = get_collections_files()

    for i, f in enumerate(_files):
        try:
            file_id = f['fileinfo'].get('_id', '')
            if file_id:
                file_path = util.path_from_uuid(file_id)
                if not config.fs.isfile(file_path):
                    """Copy file from legacy to new storage"""

                    log.debug('copy file between the legacy and new storage: %s' % file_path)

                    dst_dir = fs.path.dirname(file_path)
                    config.fs.makedirs(dst_dir, recreate=True)
                    fs.move.copy_file(src_fs=config.legacy_fs, src_path=file_path, dst_fs=config.fs, dst_path=file_path)
            else:
                """generate uuuid, set the id field and copy the file"""
                file_id = str(uuid.uuid4())
                f_old_path = util.path_from_hash(f['fileinfo']['hash'])
                f_new_path = util.path_from_uuid(file_id)

                log.debug('copy file %s to %s' % (f_old_path, f_new_path))

                dst_dir = fs.path.dirname(f_new_path)
                config.fs.makedirs(dst_dir, recreate=True)
                fs.move.copy_file(src_fs=config.legacy_fs, src_path=f_old_path, dst_fs=config.fs, dst_path=f_new_path)

                update_set = {
                    f['prefix'] + '.$.modified': datetime.datetime.utcnow(),
                    f['prefix'] + '.$._id': file_id
                }
                log.debug('update file in mongo: %s' % update_set)
                # Update the file with the newly generated UUID
                updated_doc = config.db[f['collection']].find_one_and_update(
                    {'_id': f['collection_id'], f['prefix'] + '.name': f['fileinfo']['name'],
                     f['prefix'] + '.hash': f['fileinfo']['hash']},
                    {'$set': update_set}
                )

                if not updated_doc:
                    log.info('Probably the following file has been updated during the migration '
                                'and its hash is changed, cleaning up from the new filesystem')
                    config.fs.remove(f_new_path)

            show_progress(i + 1, len(_files))

        except Exception as e:
            log.exception(e)
            raise MigrationError('Wasn\'t able to migrate the \'%s\' '
                                 'file in the \'%s\' collection (collection id: %s)' %
                                 (f['fileinfo']['name'], f['collection'], str(f['collection_id'])), e)


def migrate_gears():
    log.info('Migrate gears...')

    _files = get_gears_files()

    for i, f in enumerate(_files):
        try:
            file_id = f['exchange'].get('rootfs-id', '')
            if file_id:
                file_path = util.path_from_uuid(file_id)
                if not config.fs.isfile(file_path):
                    """Copy file from legacy to new storage"""

                    log.debug('copy file between the legacy and new storage: %s' % file_path)

                    dst_dir = fs.path.dirname(file_path)
                    config.fs.makedirs(dst_dir, recreate=True)
                    fs.move.copy_file(src_fs=config.legacy_fs, src_path=file_path, dst_fs=config.fs, dst_path=file_path)
            else:
                file_id = str(uuid.uuid4())
                file_hash = 'v0-' + f['exchange']['rootfs-hash'].replace(':', '-')
                f_old_path = util.path_from_hash(file_hash)
                f_new_path = util.path_from_uuid(file_id)

                log.debug('copy file %s to %s' % (f_old_path, f_new_path))

                dst_dir = fs.path.dirname(f_new_path)
                config.fs.makedirs(dst_dir, recreate=True)
                fs.move.copy_file(src_fs=config.legacy_fs, src_path=f_old_path, dst_fs=config.fs, dst_path=f_new_path)

                update_set = {
                    'modified': datetime.datetime.utcnow(),
                    'exchange.rootfs-id': file_id
                }

                log.debug('update file in mongo: %s' % update_set)
                # Update the gear with the newly generated UUID
                config.db['gears'].find_one_and_update(
                    {'_id': f['gear_id']},
                    {'$set': update_set}
                )

                show_progress(i + 1, len(_files))
        except Exception as e:
            log.exception(e)
            raise MigrationError('Wasn\'t able to migrate the \'%s\' gear (gear id: %s)' %
                                 (f['gear_name'], f['gear_id']), e)


def migrate_storage():
    """
    Remove CAS logic, generate UUID for the files and move the files from the lagacy storage to the new one.
    """

    parser = argparse.ArgumentParser(prog='Migrate storage')
    parser.add_argument('--collections', action='store_true', help='Migrate collections')
    parser.add_argument('--gears', action='store_true', help='Migrate gears')
    parser.add_argument('--delete-files', action='store_true', help='Delete files from legacy storage')


    args = parser.parse_args()

    try:
        if not (args.collections or args.gears):
            migrate_collections()
            migrate_gears()

        if args.collections:
            migrate_collections()

        if args.gears:
            migrate_gears()

        if args.delete_files:
            config.legacy_fs.removetree('/')

    except MigrationError as e:
        log.exception(e)
        exit(1)
    finally:
        cleanup_empty_folders()
        pass


if __name__ == '__main__':
    migrate_storage()
