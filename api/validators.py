import os
import copy
import json
import logging
import jsonschema

log = logging.getLogger('scitran.api')
# following https://github.com/Julian/jsonschema/issues/98
# json schema files are expected to be in the schemas folder relative to this module
schema_path = os.path.abspath(os.path.dirname(__file__))

resolver = jsonschema.RefResolver('file://' + schema_path + '/schemas/', None)

expected_schemas = set([
    'acquisition.json',
    'collection.json',
    'container.json',
    'file.json',
    'group.json',
    'note.json',
    'permission.json',
    'project.json',
    'session.json',
    'subject.json',
    'user.json',
    'avatars.json'
 ])
mongo_schemas = set()
input_schemas = set()
# validate and cache schemas at start time
for schema_file in os.listdir(schema_path + '/schemas/mongo/'):
    mongo_schemas.add(schema_file)
    resolver.resolve('mongo/' + schema_file)

assert mongo_schemas == expected_schemas, '{} is different from {}'.format(mongo_schemas, expected_schemas)

for schema_file in os.listdir(schema_path + '/schemas/input/'):
    input_schemas.add(schema_file)
    resolver.resolve('input/' + schema_file)

assert input_schemas == expected_schemas, '{} is different from {}'.format(input_schemas, expected_schemas)

def _validate_json(json_data, schema):
    jsonschema.validate(json_data, schema, resolver=resolver)
    #jsonschema.Draft4Validator(schema, resolver=resolver).validate(json_data)

def no_op(g, *args):
    return g

def mongo_from_schema_file(handler, schema_file):
    if schema_file is None:
        return no_op
    schema = resolver.resolve(schema_file)[1]
    def g(exec_op):
        def f(method, **kwargs):
            payload = kwargs['payload']
            log.warn(payload)
            if method == 'PUT' and schema.get('required'):
                _schema = copy.copy(schema)
                _schema.pop('required')
            else:
                _schema = schema
            if method in ['POST', 'PUT']:
                try:
                    _validate_json(payload, _schema)
                except jsonschema.ValidationError as e:
                    handler.abort(500, str(e))
            return exec_op(method, **kwargs)
        return f
    return g

def payload_from_schema_file(handler, schema_file):
    if schema_file is None:
        return no_op
    schema = resolver.resolve(schema_file)[1]
    def g(payload, method):
        if method == 'PUT' and schema.get('required'):
            _schema = copy.copy(schema)
            _schema.pop('required')
        else:
            _schema = schema
        if method in ['POST', 'PUT']:
            try:
                _validate_json(payload, _schema)
            except jsonschema.ValidationError as e:
                handler.abort(400, str(e))
    return g

def key_check(handler, schema_file):
    if schema_file is None:
        return no_op
    schema = resolver.resolve(schema_file)[1]
    log.debug(schema)
    if schema.get('key_fields') is None:
        return no_op
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None, exclude_params=None):
            if method == 'POST':
                exclude_params = _post_exclude_params(schema.get('key_fields', []), payload)
            else:
                _check_query_params(schema.get('key_fields'), query_params)
                if method == 'PUT' and schema.get('key_fields'):
                    exclude_params = _put_exclude_params(schema['key_fields'], query_params, payload)
            return exec_op(method, _id=_id, query_params=query_params, payload=payload, exclude_params=exclude_params)
        return f
    return g

def _put_exclude_params(keys, query_params, payload):
    exclude_params = None
    _eqp = {}
    for k in keys:
        value_p = payload.get(k)
        if value_p and value_p != query_params.get(k):
            _eqp[k] = value_p
            exclude_params = _eqp
        else:
            _eqp[k] = query_params.get(k)
    return exclude_params

def _post_exclude_params(keys, payload):
    try:
        exclude_params = {
            k: payload[k] for k in keys
        }
    except KeyError:
        raise KeyError('missing key {} in payload'.format(k))
    return exclude_params

def _check_query_params(keys, query_params):
    set_keys = set(keys)
    set_query_params_keys = set(query_params.keys())
    assert set(keys) == set(query_params.keys()), """
    {}
    is different from expected:
    {}
    """.format(query_params.keys(), keys)
