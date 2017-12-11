import logging

import jsonschema.exceptions
import pytest
import fnmatch, json, os, os.path, re

from api import config, validators

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

# Enable to force failure if example data is missing
FAIL_ON_MISSING_EXAMPLE = False

SCHEMAS_PATH = config.schema_path + '/'
EXAMPLES_PATH = os.path.join(SCHEMAS_PATH, '../examples')
LIST_SCHEMA = re.compile(r'(\w+)-list')

class StubHandler:
    def abort(iself, code, message):
        err_m = str(code) + ' ' + message
        raise Exception(err_m)

default_handler = StubHandler()

def test_payload():
    payload = {
        'files': [],
        'group': 'unknown',
        'label': 'SciTran/Testing',
        'public': False,
        'permissions': [],
        'extra_params': 'testtest'
    }
    schema_uri = validators.schema_uri("input", "project.json")
    schema, resolver = validators._resolve_schema(schema_uri)
    with pytest.raises(jsonschema.exceptions.ValidationError):
        validators._validate_json(payload, schema, resolver)

# Parametrized test that example payloads are valid
def test_example_payload_valid(schema_type, schema_name):
    example_data = load_example_data(schema_type, schema_name)
    if FAIL_ON_MISSING_EXAMPLE:
        assert example_data is not None

    if example_data is not None:
        schema_uri = validators.schema_uri(schema_type, '{0}.json'.format(schema_name))
        schema, resolver = validators._resolve_schema(schema_uri)
        validators._validate_json(example_data, schema, resolver)
    
# Generate unit tests for all schema files
# These tests fill fail if examples are missing
def pytest_generate_tests(metafunc):
    if 'schema_type' not in metafunc.fixturenames:
        return

    schema_files = []

    # Collect all schema files
    for root, dirs, files in os.walk(SCHEMAS_PATH):
        for filename in files:
            if fnmatch.fnmatch(filename, '*.json'):
                path = os.path.join(root, filename)
                relpath = path[len(SCHEMAS_PATH):]     
                schema_files.append( relpath )

    test_args = []
    for relpath in schema_files:        
        # Get schema path, and test name
        schema_type, schema_name = os.path.split(relpath)
        if schema_type == 'input' or schema_type == 'output':
            schema_name, ext = os.path.splitext(schema_name)
            test_args.append( (schema_type, schema_name) )

    metafunc.parametrize('schema_type,schema_name', test_args)

# Helper to load the example data from a file
def load_example_data(schema_type, schema_name):
    example_path = os.path.join(EXAMPLES_PATH, schema_type, '{0}.json'.format(schema_name))
    example_data = None

    if os.path.exists(example_path):
        with open(example_path) as example_file:
            example_data = json.load(example_file)
    else:
        m = LIST_SCHEMA.match(schema_name)
        if m is not None:
            obj_path = os.path.join(EXAMPLES_PATH, '{0}.json', m.group(1))
            if os.path.exists(obj_path):
                with open(obj_path) as example_file:
                    example_data = [json.load(example_file)]

    return example_data


