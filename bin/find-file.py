#!/usr/bin/env python

import argparse
import logging
import sys
import traceback



from api import config





def list_found_files(file_id):
    file_locations = []

    # Acquisition files
    query = {"files.hash" : file_id}
    acq_results = config.db.acquisitions.find(query)
    for acq_candidate in acq_results:
        for file_candidate in acq_candidate.get('files'):
            if file_candidate.get('hash') == file_id:

                session_id = acq_candidate.get('session')
                file_locations.append(
                    '{0}/acquisitions/{1}/files/{2}'.format(
                        get_session_hiearchy_string(session_id),
                        acq_candidate.get('label'),
                        file_candidate.get('name')
                    )
                )


    # Session Attachments
    query = {"files.hash" : file_id}
    sess_results = config.db.sessions.find(query)
    for result in sess_results:
        for file_candidate in result.get('files'):
            if file_candidate.get('hash') == file_id:
                file_locations.append(
                    '{0}/sessions/{1}/files/{2}'.format(
                        get_project_hiearchy_string(result.get('project')),
                        result.get('_id'),
                        file_candidate.get('name')
                    )
                )


    # Analyses Results
    query = {"analyses.files.hash" : file_id}
    sess_results = config.db.sessions.find(query)
    for result in sess_results:
        for analyses_candidate in result.get('analyses'):
            for file_candidate in analyses_candidate.get('files'):
                if file_candidate.get('hash') == file_id:
                    file_locations.append(
                        '{0}/sessions/{1}/analyses/{2}/files/{3}'.format(
                            get_project_hiearchy_string(result.get('project')),
                            result.get('_id'),
                            analyses_candidate.get('_id'),
                            file_candidate.get('name')
                        )
                    )


    # Project Attachments
    query = {"files.hash" : file_id}
    proj_results = config.db.projects.find(query)
    for result in proj_results:
        for file_candidate in result.get('files'):
            if file_candidate.get('hash') == file_id:
                file_locations.append(
                    '{0}/projects/{1}/files/{2}'.format(
                        get_group_hiearchy_string(result.get('group')),
                        result.get('_id'),
                        file_candidate.get('name')
                    )
                )


    # Job inputs

    #


    return file_locations

def get_session_hiearchy_string(session_id):
    session_doc = config.db.sessions.find_one({ "_id" : session_id })
    session_label = session_doc.get('label')
    project_id = session_doc.get('project')


    return '{0}/sessions/{1}'.format(
        get_project_hiearchy_string(project_id),
        session_label
    )

def get_project_hiearchy_string(project_id):


    project_doc = config.db.projects.find_one({"_id" : project_id })
    project_label = project_doc.get('label')
    group_id = project_doc.get('group')


    return '{0}/projects/{1}'.format(
        get_group_hiearchy_string(group_id),
        project_label
    )


def get_group_hiearchy_string(group_id):
    group_label = config.db.groups.find_one({"_id" : group_id }).get('name')

    return '/groups/{0}'.format(
        group_label
    )


def print_file_with_hiearchy(container_type, container_id, path_to_file_list):
    '''

    Job submisson provide: container type, container id, file name

    Jobs calculate "request" with path plus name which is valid download endpoint.
        uri : "/sessions/57977f90d591ca0020998aef/analyses/5797d89510db46002203c562/files/aseg.stats.table.csv"
        uri : "/acquisitions/57977f90d591ca0020998af0/files/3Plane Loc SSFSE.dicom.zip"


    Valid container_type/path_to_file_list
        acquisitions/files
        sessions/files
        sessions/analyses.{_id}.files
        projects/files

    '''



ap = argparse.ArgumentParser()
ap.description = 'Find references to files in the database'
ap.add_argument('file_id', help='File id')
args = ap.parse_args()




try:
    for file_match in list_found_files(args.file_id):
        print('{1}'.format(args.file_id, file_match))
except Exception as ex:
    template = "An exception of type {0} occured. Arguments:\n{1!r}"
    message = template.format(type(ex).__name__, ex.args)
    logging.error(message)
    logging.error(traceback.format_exc())
    sys.exit(1)
