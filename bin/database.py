#!/usr/bin/env python

import bson
import copy
import dateutil.parser
import json
import logging
import sys

from api import config
from api.dao import containerutil
from api.jobs.jobs import Job

CURRENT_DATABASE_VERSION = 13 # An int that is bumped when a new schema change is made

def get_db_version():

    version = config.get_version()
    if version is None:
        # Attempt to find db version at old location
        version = config.db.version.find_one({'_id': 'version'})
    if version is None or version.get('database') is None:
        return 0
    return version.get('database')


def confirm_schema_match():
    """
    Checks version of database schema

    Returns (0)  if DB schema version matches requirements.
    Returns (42) if DB schema version does not match
                 requirements and can be upgraded.
    Returns (43) if DB schema version does not match
                 requirements and cannot be upgraded,
                 perhaps because code is at lower version
                 than the DB schema version.
    """

    db_version = get_db_version()
    if not isinstance(db_version, int) or db_version > CURRENT_DATABASE_VERSION:
        logging.error('The stored db schema version of %s is incompatible with required version %s',
                       str(db_version), CURRENT_DATABASE_VERSION)
        sys.exit(43)
    elif db_version < CURRENT_DATABASE_VERSION:
        sys.exit(42)
    else:
        sys.exit(0)

def upgrade_to_1():
    """
    scitran/core issue #206

    Initialize db version to 1
    """
    config.db.singletons.insert_one({'_id': 'version', 'database': 1})

def upgrade_to_2():
    """
    scitran/core PR #236

    Set file.origin.name to id if does not exist
    Set file.origin.method to '' if does not exist
    """

    def update_file_origins(cont_list, cont_name):
        for container in cont_list:
            updated_files = []
            for file in container.get('files', []):
                origin = file.get('origin')
                if origin is not None:
                    if origin.get('name', None) is None:
                        file['origin']['name'] = origin['id']
                    if origin.get('method', None) is None:
                        file['origin']['method'] = ''
                updated_files.append(file)

            query = {'_id': container['_id']}
            update = {'$set': {'files': updated_files}}
            result = config.db[cont_name].update_one(query, update)

    query = {'$and':[{'files.origin.name': { '$exists': False}}, {'files.origin.id': { '$exists': True}}]}

    update_file_origins(config.db.collections.find(query), 'collections')
    update_file_origins(config.db.projects.find(query), 'projects')
    update_file_origins(config.db.sessions.find(query), 'sessions')
    update_file_origins(config.db.acquisitions.find(query), 'acquisitions')

def upgrade_to_3():
    """
    scitran/core issue #253

    Set first user with admin permissions found as curator if one does not exist
    """
    query = {'curator': {'$exists': False}, 'permissions.access': 'admin'}
    projection = {'permissions.$':1}
    collections = config.db.collections.find(query, projection)
    for coll in collections:
        admin = coll['permissions'][0]['_id']
        query = {'_id': coll['_id']}
        update = {'$set': {'curator': admin}}
        config.db.collections.update_one(query, update)

def upgrade_to_4():
    """
    scitran/core issue #263

    Add '_id' field to session.subject
    Give subjects with the same code and project the same _id
    """

    pipeline = [
        {'$match': { 'subject._id': {'$exists': False}}},
        {'$group' : { '_id' : {'pid': '$project', 'code': '$subject.code'}, 'sids': {'$push': '$_id' }}}
    ]

    subjects = config.db.command('aggregate', 'sessions', pipeline=pipeline)
    for subject in subjects['result']:

        # Subjects without a code and sessions without a subject
        # will be returned grouped together, but all need unique IDs
        if subject['_id'].get('code') is None:
            for session_id in subject['sids']:
                subject_id = bson.ObjectId()
                config.db.sessions.update_one({'_id': session_id},{'$set': {'subject._id': subject_id}})
        else:
            subject_id = bson.ObjectId()
            query = {'_id': {'$in': subject['sids']}}
            update = {'$set': {'subject._id': subject_id}}
            config.db.sessions.update_many(query, update)

def upgrade_to_5():
    """
    scitran/core issue #279

    Ensure all sessions and acquisitions have the same perms as their project
    Bug(#278) discovered where changing a session's project did not update acquisition perms
    """

    projects = config.db.projects.find({})
    for p in projects:
        perms = p.get('permissions', [])

        session_ids = [s['_id'] for s in config.db.sessions.find({'project': p['_id']}, [])]

        config.db.sessions.update_many({'project': p['_id']}, {'$set': {'permissions': perms}})
        config.db.acquisitions.update_many({'session': {'$in': session_ids}}, {'$set': {'permissions': perms}})

def upgrade_to_6():
    """
    scitran/core issue #277

    Ensure all collection modified dates are ISO format
    Bug fixed in 6967f23
    """

    colls = config.db.collections.find({'modified': {'$type': 2}}) # type string
    for c in colls:
        fixed_mod = dateutil.parser.parse(c['modified'])
        config.db.collections.update_one({'_id': c['_id']}, {'$set': {'modified': fixed_mod}})

def upgrade_to_7():
    """
    scitran/core issue #270

    Add named inputs and specified destinations to jobs.

    Before:
    {
        "input" : {
            "container_type" : "acquisition",
            "container_id" : "572baf4e23dcb77ebbe06b3f",
            "filename" : "1_1_dicom.zip",
            "filehash" : "v0-sha384-422bd115d21585d1811d42cd99f1cf0a8511a4b377dd2deeaa1ab491d70932a051926ed99815a75142ad0815088ed009"
        }
    }

    After:
    {
        "inputs" : {
            "dicom" : {
                "container_type" : "acquisition",
                "container_id" : "572baf4e23dcb77ebbe06b3f",
                "filename" : "1_1_dicom.zip"
            }
        },
        "destination" : {
            "container_type" : "acquisition",
            "container_id" : "572baf4e23dcb77ebbe06b3f"
        }
    }
    """

    # The infrastructure runs this upgrade script before populating manifests.
    # For this reason, this one-time script does NOT pull manifests to do the input-name mapping, instead relying on a hard-coded alg name -> input name map.
    # If you have other gears in your system at the time of upgrade, you must add that mapping here.
    input_name_for_gear = {
        'dcm_convert': 'dicom',
        'qa-report-fmri': 'nifti',
        'dicom_mr_classifier': 'dicom',
    }

    jobs = config.db.jobs.find({'input': {'$exists': True}})

    for job in jobs:
        gear_name = job['algorithm_id']
        input_name = input_name_for_gear[gear_name]

        # Move single input to named input map
        input_ = job['input']
        input_.pop('filehash', None)
        inputs = { input_name: input_ }

        # Destination is required, and (for these jobs) is always the same container as the input
        destination = copy.deepcopy(input_)
        destination.pop('filename', None)

        config.db.jobs.update_one(
            {'_id': job['_id']},
            {
                '$set': {
                    'inputs': inputs,
                    'destination': destination
                },
                '$unset': {
                    'input': ''
                }
            }
        )

def upgrade_to_8():
    """
    scitran/core issue #291

    Migrate config, version, gears and rules to singletons collection
    """

    colls = config.db.collection_names()
    to_be_removed = ['version', 'config', 'static']
    # If we are in a bad state (singletons exists but so do any of the colls in to be removed)
    # remove singletons to try again
    if 'singletons' in colls and set(to_be_removed).intersection(set(colls)):
        config.db.drop_collection('singletons')

    if 'singletons' not in config.db.collection_names():
        static = config.db.static.find({})
        if static.count() > 0:
            config.db.singletons.insert_many(static)
        config.db.singletons.insert(config.db.version.find({}))

        configs = config.db.config.find({'latest': True},{'latest':0})
        if configs.count() == 1:
            c = configs[0]
            c['_id'] = 'config'
            config.db.singletons.insert_one(c)

        for c in to_be_removed:
            if c in config.db.collection_names():
                config.db.drop_collection(c)


def upgrade_to_9():
    """
    scitran/core issue #292

    Remove all session and acquisition timestamps that are empty strings
    """

    config.db.acquisitions.update_many({'timestamp':''}, {'$unset': {'timestamp': ''}})
    config.db.sessions.update_many({'timestamp':''}, {'$unset': {'timestamp': ''}})

def upgrade_to_10():
    """
    scitran/core issue #301

    Makes the following key renames, all in the jobs table.
    FR is a FileReference, CR is a ContainerReference:

    job.algorithm_id  --> job.name

    FR.container_type --> type
    FR.container_id   --> id
    FR.filename       --> name

    CR.container_type --> type
    CR.container_id   --> id
    """

    def switch_keys(doc, x, y):
        doc[y] = doc[x]
        doc.pop(x, None)


    jobs = config.db.jobs.find({'destination.container_type': {'$exists': True}})

    for job in jobs:
        switch_keys(job, 'algorithm_id', 'name')

        for key in job['inputs'].keys():
            inp = job['inputs'][key]

            switch_keys(inp, 'container_type', 'type')
            switch_keys(inp, 'container_id',   'id')
            switch_keys(inp, 'filename',       'name')


        dest = job['destination']
        switch_keys(dest, 'container_type', 'type')
        switch_keys(dest, 'container_id',   'id')

        config.db.jobs.update(
            {'_id': job['_id']},
            job
        )

def upgrade_to_11():
    """
    scitran/core issue #363

    Restructures job objects' `inputs` field from a dict with arbitrary keys
    into a list where the key becomes the field `input`
    """

    jobs = config.db.jobs.find({'inputs.type': {'$exists': False}})

    for job in jobs:

        inputs_arr = []
        for key, inp in job['inputs'].iteritems():
            inp['input'] = key
            inputs_arr.append(inp)

        config.db.jobs.update(
            {'_id': job['_id']},
            {'$set': {'inputs': inputs_arr}}
        )

def upgrade_to_12():
    """
    scitran/core PR #372

    Store job inputs on job-based analyses
    """

    sessions = config.db.sessions.find({'analyses.job': {'$exists': True}})

    for session in sessions:
        for analysis in session.get('analyses'):
            if analysis.get('job'):
                job = Job.get(analysis['job'])
                files = analysis.get('files', [])
                files[:] = [x for x in files if x.get('output')] # remove any existing inputs and insert fresh

                for i in getattr(job, 'inputs', {}):
                    fileref = job.inputs[i]
                    contref = containerutil.create_containerreference_from_filereference(job.inputs[i])
                    file_ = contref.find_file(fileref.name)
                    if file_:
                        file_['input'] = True
                        files.append(file_)

                q = {'analyses._id': analysis['_id']}
                u = {'$set': {'analyses.$.job': job.id_, 'analyses.$.files': files}}
                config.db.sessions.update_one(q, u)

def upgrade_to_13():
    """
    scitran/core PR #403

    Clear schema path from db config in order to set abs path to files
    """
    config.db.singletons.find_one_and_update(
        {'_id': 'config', 'persistent.schema_path': {'$exists': True}},
        {'$unset': {'persistent.schema_path': ''}})


def upgrade_schema():
    """
    Upgrades db to the current schema version

    Returns (0) if upgrade is successful
    """

    db_version = get_db_version()
    try:
        if db_version < 1:
            upgrade_to_1()
        if db_version < 2:
            upgrade_to_2()
        if db_version < 3:
            upgrade_to_3()
        if db_version < 4:
            upgrade_to_4()
        if db_version < 5:
            upgrade_to_5()
        if db_version < 6:
            upgrade_to_6()
        if db_version < 7:
            upgrade_to_7()
        if db_version < 8:
            upgrade_to_8()
        if db_version < 9:
            upgrade_to_9()
        if db_version < 10:
            upgrade_to_10()
        if db_version < 11:
            upgrade_to_11()
        if db_version < 12:
            upgrade_to_12()
        if db_version < 13:
            upgrade_to_13()

    except Exception as e:
        logging.exception('Incremental upgrade of db failed')
        sys.exit(1)
    else:
        config.db.singletons.update_one({'_id': 'version'}, {'$set': {'database': CURRENT_DATABASE_VERSION}})
        sys.exit(0)

try:
    if len(sys.argv) > 1:
        if sys.argv[1] == 'confirm_schema_match':
            confirm_schema_match()
        elif sys.argv[1] == 'upgrade_schema':
            upgrade_schema()
        else:
            logging.error('Unknown method name given as argv to database.py')
            sys.exit(1)
    else:
        logging.error('No method name given as argv to database.py')
        sys.exit(1)
except Exception as e:
    logging.exception('Unexpected error in database.py')
    sys.exit(1)
