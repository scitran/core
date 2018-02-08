import logging

import jsonschema.exceptions
import pytest
import fnmatch, json, os, os.path, re

from api import config, validators

log = logging.getLogger(__name__)
sh = logging.StreamHandler()
log.addHandler(sh)

# Enable to force failure if example data is missing
FAIL_ON_MISSING_EXAMPLE = True

SCHEMAS_PATH = config.schema_path + '/'
EXAMPLES_PATH = os.path.join(SCHEMAS_PATH, '../examples')
LIST_SCHEMA = re.compile(r'(\w+)-list')

# These schemas will not fail if there is no example
IGNORED_SCHEMAS = [
    'input/container.json',
    'input/enginemetadata.json', # TODO: Needs an example, is the schema really up to date?
    'input/search.json', # TODO: Is this used?

    'output/sites-list.json' #TODO: Is this used?
]

# In the event that there is a suitable example in a location other than /examples/{type}/{name}.json,
# Add a mapping entry to this collection
EXAMPLES_MAP = {
    'input/download.json': 'create_download_incomplete_and_dicom.json',

    'output/config.json': 'scitran_config.json',
    'output/file-list.json': 'file_info_list.json',
    'output/gears-list.json': 'gears_list_just_name.json',
    'output/user-list.json': 'user-list.json',
    'output/user-self.json': 'user_jane_doe.json',
    'output/user.json': 'user_jane_doe.json'
}

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

def test_file_output_valid():
    payload = [{
        'modified': 'yesterday',
        'size': 10
    }]
    schema_uri = validators.schema_uri("output", "file-list.json")
    schema, resolver = validators._resolve_schema(schema_uri)
    validators._validate_json(payload, schema, resolver)

def test_file_output_invalid():
    payload = [{
        'modified': 'yesterday'
    }]
    schema_uri = validators.schema_uri("output", "file-list.json")
    schema, resolver = validators._resolve_schema(schema_uri)
    with pytest.raises(jsonschema.exceptions.ValidationError):
        validators._validate_json(payload, schema, resolver)
    

# ===== Automated Tests =====

# Parametrized test that example payloads are valid
def test_example_payload_valid(schema_type, schema_name):
    example_data = load_example_data(schema_type, schema_name)
    if example_data is None:
        if FAIL_ON_MISSING_EXAMPLE:
            pytest.fail('Missing example file for: {0}/{1}.json'.format(schema_type, schema_name))
    else:
        schema_uri = validators.schema_uri(schema_type, '{0}.json'.format(schema_name))
        schema, resolver = validators._resolve_schema(schema_uri)
        validators._validate_json(example_data, schema, resolver)
    
# Generate unit tests for all schema files
# These tests will fail if examples are missing
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
                if relpath not in IGNORED_SCHEMAS:
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
    example_path = None
    example_data = None
    # First check if there is example data in the schema file
    relpath = os.path.join(SCHEMAS_PATH, schema_type, '{0}.json'.format(schema_name))
    if os.path.exists(relpath):
        with open(relpath) as f:
            schema_data = json.load(f)
        if schema_data and 'example' in schema_data:
            example_data = schema_data['example']
            if '$ref' in example_data:
                # Resolve ref
                example_path = os.path.join(relpath, example_data['$ref'])
                example_data = None
            else:
                return schema_data['example']

    # Then check in the examples folder
    if example_path is None or not os.path.exists(example_path):
        relpath = os.path.join(schema_type, '{0}.json'.format(schema_name))

        if relpath in EXAMPLES_MAP:
            example_path = os.path.join(EXAMPLES_PATH, EXAMPLES_MAP[relpath])
        else:
            example_path = os.path.join(EXAMPLES_PATH, relpath)

    if os.path.exists(example_path):
        with open(example_path) as f:
            example_data = json.load(f)
    else:
        m = LIST_SCHEMA.match(schema_name)
        if m is not None:
            obj_path = os.path.join(EXAMPLES_PATH, '{0}.json', m.group(1))
            if os.path.exists(obj_path):
                with open(obj_path) as f:
                    example_data = [json.load(f)]

    return example_data


