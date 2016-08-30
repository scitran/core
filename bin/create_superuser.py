#!/usr/bin/env python

"""This script creates a superuser"""

import base64
import os
import os.path
import sys
import json
import logging
import argparse
import datetime

import jsonschema

repo_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))
sys.path.append(repo_path)

from api import config, validators

def create_superuser(json_file_path):
    # This is the best we can do at this time,
    # without writing code in the API
    user_schema_path = validators.schema_uri("mongo", "user.json")
    user_schema, user_resolver = validators._resolve_schema(user_schema_path)
    now = datetime.datetime.utcnow()
    with open(json_file_path, "r") as user_input_file:
        user = json.load(user_input_file)
        user["api_key"] = {
            "key":base64.urlsafe_b64encode(os.urandom(42)),
            "created":now,
            "last_used":now
        }
        user["created"] = now
        user["modified"] = now
        user["root"] = True
    validators._validate_json(user, user_schema, user_resolver)
    config.db.users.insert_one(user)
    return user

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.description = 'Create a superuser and print the API key'
    arg_parser.add_argument('json', help="JSON file containing a user to add")
    args = arg_parser.parse_args()
    user = create_superuser(args.json)
    print(user["api_key"]["key"])
