#!/usr/bin/env python

"""This script helps bootstrap users and data"""

import os
import os.path
import sys
import json
import logging
import argparse
import datetime

import jsonschema

from api import config, validators
from api.handlers.userhandler import UserHandler
from api.dao.containerstorage import ContainerStorage

def set_api_key(user_id, api_key):
    """Sets an API key for a user in the database

    Overwrites existing keys and return success if existing key matches. Raises
    exception if matching user cannot be found, or database record of matching
    user fails to be updated.

    Args:
        user_id (str): ID of the user to reset the api key for
        api_key (str): api_key to set for given user
    """
    users_storage = ContainerStorage('users', use_object_id=False)
    api_key_doc = {
        "api_key":{
            "key":api_key
        }
    }
    matching_user = users_storage.exec_op('GET', _id=user_id)
    if matching_user is None:
        raise RuntimeError("Unable to find matching user: " + user_id)

    if matching_user['api_key'] == api_key_doc['api_key']:
        return

    result = users_storage.exec_op('PUT', _id=user_id, payload=api_key_doc)
    if result.modified_count != 1:
        raise RuntimeError("Unable to set API key for user")

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.description = 'Bootstrap SciTran users and groups'
    ap.add_argument('user_id', help='ID of User to set API key for')
    ap.add_argument("api_key", help="API key to set for given user")
    args = ap.parse_args()
    set_api_key(args.user_id, args.api_key)
