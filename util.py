# @author:  Gunnar Schaefer

import logging
log = logging.getLogger('scitran.api')

import os
import copy
import shutil
import difflib
import datetime
import tempdir as tempfile

import scitran.data

PROJECTION_FIELDS = ['timestamp', 'permissions', 'public']


def insert_file(dbc, _id, file_info, filepath, digest, data_path, quarantine_path, flavor='file'):
    """Insert a file as an attachment or as a file."""
    filename = os.path.basename(filepath)
    flavor += 's'
    if _id is None:
        try:
            log.info('Parsing     %s' % filename)
            dataset = scitran.data.parse(filepath)
        except scitran.data.DataError:
            q_path = tempfile.mkdtemp(prefix=datetime.datetime.now().strftime('%Y%m%d_%H%M%S_'), dir=quarantine_path)
            shutil.move(filepath, q_path)
            return 202, 'Quarantining %s (unparsable)' % filename

        log.info('Sorting     %s' % filename)
        _id = _update_db(dbc.database, dataset)
        file_spec = dict(
                _id=_id,
                files={'$elemMatch': {
                    'type': dataset.nims_file_type,
                    'kinds': dataset.nims_file_kinds,
                    'state': dataset.nims_file_state,
                    }},
                )
        file_info = dict(
                name=dataset.nims_file_name,
                ext=dataset.nims_file_ext,
                size=os.path.getsize(filepath),
                sha1=digest,
                #hash=dataset.nims_hash, TODO: datasets should be able to hash themselves (but not here)
                type=dataset.nims_file_type,
                kinds=dataset.nims_file_kinds,
                state=dataset.nims_file_state,
                )
        filename = dataset.nims_file_name + dataset.nims_file_ext
    else:
        file_spec = {
                '_id': _id,
                flavor: {'$elemMatch': {
                    'type': file_info.get('type'),
                    'kinds': file_info.get('kinds'),
                    'state': file_info.get('state'),
                    }},
                }
        if flavor == 'attachments':
            file_spec[flavor]['$elemMatch'].update({'name': file_info.get('name'), 'ext': file_info.get('ext')})
    container_path = os.path.join(data_path, str(_id)[-3:] + '/' + str(_id))
    if not os.path.exists(container_path):
        os.makedirs(container_path)
    success = dbc.update(file_spec, {'$set': {flavor + '.$': file_info}})
    if not success['updatedExisting']:
        dbc.update({'_id': _id}, {'$push': {flavor: file_info}})
    shutil.move(filepath, container_path + '/' + filename)
    log.debug('Done        %s' % os.path.basename(filepath)) # must use filepath, since filename is updated for sorted files
    return 200, 'Success'


def _update_db(db, dataset):
    #TODO: possibly try to keep a list of session IDs on the project, instead of having the session point to the project
    #      same for the session and acquisition
    #      queries might be more efficient that way
    session_spec = {'uid': dataset.nims_session_id}
    session = db.sessions.find_one(session_spec, ['project'])
    if session: # skip project creation, if session exists
        project = db.projects.find_one({'_id': session['project']}, fields=PROJECTION_FIELDS)
    else:
        existing_group_ids = [g['_id'] for g in db.groups.find(None, ['_id'])]
        group_id_matches = difflib.get_close_matches(dataset.nims_group_id, existing_group_ids, cutoff=0.8)
        if len(group_id_matches) == 1:
            group_id = group_id_matches[0]
            project_name = dataset.nims_project or 'untitled'
        else:
            group_id = 'unknown'
            project_name = dataset.nims_group_id + ('/' + dataset.nims_project if dataset.nims_project else '')
        group = db.groups.find_one({'_id': group_id})
        project_spec = {'group_id': group['_id'], 'name': project_name}
        project = db.projects.find_and_modify(
                project_spec,
                {'$setOnInsert': {'permissions': group['roles'], 'public': False, 'files': []}},
                upsert=True,
                new=True,
                fields=PROJECTION_FIELDS,
                )
    session = db.sessions.find_and_modify(
            session_spec,
            {
                '$setOnInsert': dict(project=project['_id'], permissions=project['permissions'], public=project['public'], files=[]),
                '$set': _entity_metadata(dataset, dataset.session_properties, session_spec), # session_spec ensures non-empty $set
                '$addToSet': {'domains': dataset.nims_file_domain},
                },
            upsert=True,
            new=True,
            fields=PROJECTION_FIELDS,
            )
    acquisition_spec = {'uid': dataset.nims_acquisition_id}
    acquisition = db.acquisitions.find_and_modify(
            acquisition_spec,
            {
                '$setOnInsert': dict(session=session['_id'], permissions=session['permissions'], public=session['public'], files=[]),
                '$set': _entity_metadata(dataset, dataset.acquisition_properties, acquisition_spec), # acquisition_spec ensures non-empty $set
                '$addToSet': {'types': {'$each': [{'domain': dataset.nims_file_domain, 'kind': kind} for kind in dataset.nims_file_kinds]}},
                },
            upsert=True,
            new=True,
            fields=[],
            )
    if dataset.nims_timestamp:
        db.projects.update({'_id': project['_id']}, {'$max': dict(timestamp=dataset.nims_timestamp)})
        db.sessions.update({'_id': session['_id']}, {'$min': dict(timestamp=dataset.nims_timestamp), '$set': dict(timezone=dataset.nims_timezone)})
    return acquisition['_id']


def _entity_metadata(dataset, properties, metadata={}, parent_key=''):
    metadata = copy.deepcopy(metadata)
    if dataset.nims_metadata_status is not None:
        parent_key = parent_key and parent_key + '.'
        for key, attributes in properties.iteritems():
            if attributes['type'] == 'object':
                metadata.update(_entity_metadata(dataset, attributes['properties'], parent_key=key))
            else:
                value = getattr(dataset, attributes['field']) if 'field' in attributes else None
                if value or value == 0: # drop Nones and empty iterables
                    metadata[parent_key + key] = value
    return metadata


def hrsize(size):
    if size < 1000:
        return '%d%s' % (size, 'B')
    for suffix in 'KMGTPEZY':
        size /= 1024.
        if size < 10.:
            return '%.1f%s' % (size, suffix)
        if size < 1000.:
            return '%.0f%s' % (size, suffix)
    return '%.0f%s' % (size, 'Y')


def mongo_dict(d):
    def _mongo_list(d, pk=''):
        pk = pk and pk + '.'
        return sum([_mongo_list(v, pk+k) if isinstance(v, dict) else [(pk+k, v)] for k, v in d.iteritems()], [])
    return dict(_mongo_list(d))


def user_perm(permissions, _id, site=None):
    for perm in permissions:
        if perm['_id'] == _id and perm.get('site') == site:
            return perm
    else:
        return {}


def download_ticket(type_, target, filename, size):
    import bson.json_util
    return {
            '_id': str(bson.ObjectId()), # FIXME: use better ticket ID
            'timestamp': datetime.datetime.utcnow(),
            'type': type_,
            'target': target,
            'filename': filename,
            'size': size,
            }
