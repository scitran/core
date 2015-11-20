
import logging
log = logging.getLogger('scitran.api.jobs')

import bson
import pymongo
import datetime
import fnmatch

from . import base
from . import util


#
# {
#   At least one match from this array must succeed, or array must be empty
#   "any": [
#       ["file.type",             "dicom"     ] # Match the file's type
#       ["file.name",             "*.dcm"     ] # Match a shell glob for the file name
#       ["file.measurements",     "diffusion" ] # Match any of the file's measurements
#       ["container.measurement", "diffusion" ] # Match the container's primary measurment
#       ["container.has-type",    "bvec"      ] # Match the container having any file (including this one) with this type
#   ]
#
#   All matches from array must succeed, or array must be empty
#   "all": [
#   ]
#
#   Algorithm to run if both sets of rules match
#   "alg": "dcm2nii"
# }
#

MATCH_TYPES = [
    'file.type',
    'file.name',
    'file.measurements',
    'container.measurement',
    'container.has-type'
]

def eval_match(match_type, match_param, file_, container):
    """
    Given a match entry, return if the match succeeded.
    """

    if not match_type in MATCH_TYPES:
        raise Exception('Unsupported match type ' + match_type)

    # Match the file's type
    if match_type == 'file.type':
        return file_['type'] == match_param

    # Match a shell glob for the file name
    elif match_type == 'file.name':
        return  fnmatch.fnmatch(file_['type'], match_param)

    # Match any of the file's measurements
    elif match_type == 'file.measurements':
        return match_param in file_[measurements]

    # Match the container's primary measurment
    elif match_type == 'container.measurement':
        return container['measurement'] == match_param

    # Match the container having any file (including this one) with this type
    elif match_type == 'container.has-type':
        for c_file in container['files']:
            if match_param in c_file['measurements']:
                return True

        return False

    raise Exception('Unimplemented match type ' + match_type)


def eval_rule(rule, file_, container):
    """
    Decide if a rule should spawn a job.
    """

    # Are there matches in the 'any' set?
    must_match = len(rule.get('any', [])) > 0
    has_match = False

    for match in rule.get('any', []):
        if eval_match(match[0], match[1], file_, container):
            has_match = True
            break

    # If there were matches in the 'any' array and none of them succeeded
    if must_match and not has_match:
        return False

    # Are there matches in the 'all' set?
    for match in rule.get('all', []):
        if not eval_match(match[0], match[1], file_, container):
            return False

    return True


def check_rules(db, file_, container):
    """
    Check all rules that apply to this file.
    """

    project = get_project_for_container(db, container)
    rules = project['rules']

    for rule in rules:
        if eval_rule(rule, file_, container):
            pass


# TODO: consider moving to a module that has a variety of hierarchy-management helper functions
def get_project_for_container(db, container):
    """
    Recursively walk the hierarchy until the project object is found.
    """
    if 'session' in container:
        session = db.sessions.find_one({'_id': container['session']})
        return get_project_for_container(db, session)
    elif 'project' in container:
        project = db.projects.find_one({'_id': container['project']})
        return project

    raise Exception('Hierarchy walking not implemented for container ' + container('_id'))
