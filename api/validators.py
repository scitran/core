import os
import json
import jsonschema

# following https://github.com/Julian/jsonschema/issues/98
# json schema files are expected to be in the schemas folder relative to this module
schema_path = os.path.abspath(os.curdir)
resolver = jsonschema.RefResolver('file://' + schema_path + '/api/schemas/', None)

def _validate_json(json_data, schema):
    jsonschema.Draft4Validator(schema, resolver=resolver).validate(json_data)

def no_op(g):
    return g

def from_schema_file(handler, schema_file):
    if schema_file is None:
        return no_op
    schema = resolver.resolve(schema_file)[1]
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None, uniq_params=None):
            if method == 'PUT':
                schema.pop('required')
            if method in ['POST', 'PUT']:
                try:
                    _validate_json(payload, schema)
                except jsonschema.ValidationError as e:
                    handler.abort(400, str(e))
            return exec_op(method, _id, query_params, payload, uniq_params)
        return f
    return g

def key_check(handler, schema_file):
    if schema_file is None:
        return no_op
    schema = resolver.resolve(schema_file)[1]
    if schema.get('keys') is None:
        return no_op
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None, exclude_params=None):
            if method == 'POST' and schema.get('keys'):
                exclude_params = _post_exclude_params(schema.get('keys'), payload)
            else:
                _check_query_params(schema.get('keys'), query_params)
                if method == 'PUT' and schema.get('keys'):
                    exclude_params = _put_exclude_params(schema.get('keys'), query_params, payload)
            return exec_op(method, _id, query_params, payload, exclude_params)
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
    if set(keys) != set(query_params.keys()):
        raise ValueError('unexpected query_params key set')
