"""
Gears
"""

# We shadow the standard library; this is a workaround.
from __future__ import absolute_import

import bson
import pymongo
import datetime

from collections import namedtuple
from ..dao.containerutil import FileReference, create_filereference_from_dictionary, ContainerReference, create_containerreference_from_dictionary, create_containerreference_from_filereference

from .. import base
from .. import config
from .. import util

log = config.log


def get_gears():
    """
    Fetch the install-global gears from the database
    """

    gear_doc  = config.db.static.find_one({'_id': 'gears'})
    return gear_doc['gear_list']

def get_gear_by_name(name):

    # Find a gear from the list by name
    gear_doc = config.db.static.find_one(
        {'_id': 'gears'},
        {'gear_list': { '$elemMatch': {
            'name': name
        }}
    })

    if gear_doc is None:
        raise Exception('Unknown gear ' + name)

    # Mongo returns the full document: { '_id' : 'gears', 'gear_list' : [ { .. } ] }, so strip that out
    return gear_doc['gear_list'][0]
