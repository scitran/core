import copy
import json
import jsonschema
import re
import requests

from jsonschema.compat import urlopen, urlsplit

from . import config

log = config.log

class InputValidationException(Exception):
    pass

class DBValidationException(Exception):
    pass

def validate_data(data, schema_json, schema_type, verb, optional=False):
    """
    Convenience method to validate a JSON schema against some action.

    If optional is set, validate_data won't complain about null data.
    """

    if optional and data is None:
        return

    suri = schema_uri(schema_type, schema_json)
    validator = from_schema_path(suri)
    validator(data, verb)

def _validate_json(json_data, schema, resolver):
    jsonschema.validate(json_data, schema, resolver=resolver, format_checker=jsonschema.FormatChecker())

class RefResolver(jsonschema.RefResolver):

    def resolve_remote(self, uri):
        """override default resolve_remote
        to allow testing when there is no ssl certificate
        """
        scheme = urlsplit(uri).scheme

        if scheme in self.handlers:
            result = self.handlers[scheme](uri)
        elif (
            scheme in [u"http", u"https"] and
            requests and
            getattr(requests.Response, "json", None) is not None
        ):
            # Requests has support for detecting the correct encoding of
            # json over http
            if callable(requests.Response.json):
                result = requests.get(uri, verify=False).json()
            else:
                result = requests.get(uri, verify=False).json
        else:
            # Otherwise, pass off to urllib and assume utf-8
            result = json.loads(urlopen(uri).read().decode("utf-8"))

        if self.cache_remote:
            self.store[uri] = result
        return result

# We store the resolvers for each base_uri we use, so that we reuse the schemas cached by the resolvers.
resolvers = {}
def _resolve_schema(schema_url):
    base_uri, schema_name = re.match('(.*/)(.*)', schema_url).groups()
    if not resolvers.get(base_uri):
        resolvers[base_uri] = RefResolver(base_uri, None)
    return resolvers[base_uri].resolve(schema_name)[1], resolvers[base_uri]

def no_op(g, *args):
    #pylint disable=unused-argument
    return g

def schema_uri(type_, schema_name):
    return '/'.join([
        config.get_item('site', 'api_url'),
        'schemas',
        type_, schema_name
    ])

def decorator_from_schema_path(schema_url):
    if schema_url is None:
        return no_op
    schema, resolver = _resolve_schema(schema_url)
    def g(exec_op):
        def validator(method, **kwargs):
            payload = kwargs['payload']
            log.debug(payload)
            if method == 'PUT' and schema.get('required'):
                _schema = copy.copy(schema)
                _schema.pop('required')
            else:
                _schema = schema
            if method in ['POST', 'PUT']:
                try:
                    _validate_json(payload, _schema, resolver)
                except jsonschema.ValidationError as e:
                    raise DBValidationException(str(e))
            return exec_op(method, **kwargs)
        return validator
    return g

def from_schema_path(schema_url):
    if schema_url is None:
        return no_op
    # split the url in base_uri and schema_name
    schema, resolver = _resolve_schema(schema_url)
    def g(payload, method):
        if method == 'PUT' and schema.get('required'):
            _schema = copy.copy(schema)
            _schema.pop('required')
        else:
            _schema = schema
        if method in ['POST', 'PUT']:
            try:
                _validate_json(payload, _schema, resolver)
            except jsonschema.ValidationError as e:
                raise InputValidationException(str(e))
    return g

def key_check(schema_url):
    """
    for sublists of mongo container there is no automatic key check when creating, updating or deleting an object.
    We are adding a custom array field to the json schemas ("key_fields").
    The uniqueness is checked on the combination of all the elements of "key_fields".

    For an example check api/schemas/input/permission.json:
    The key fields are the _id and the site. Uniqueness is checked on the combination
    of the values of the _id and the site of the permissions.

    So this method ensures that:
    1. after a POST and PUT request we don't have two items with the same values for the key set
    2. a GET will retrieve a single item
    3. a DELETE (most importantly) will delete a single item
    """
    if schema_url is None:
        return no_op
    schema, _ = _resolve_schema(schema_url)
    log.debug(schema)
    if schema.get('key_fields') is None:
        return no_op
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None, exclude_params=None):
            if method == 'POST':
                try:
                    exclude_params = _post_exclude_params(schema.get('key_fields', []), payload)
                except KeyError as e:
                    raise InputValidationException('missing key {} in payload'.format(e.args))
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
    return {
        k: payload[k] for k in keys
    }

def _check_query_params(keys, query_params):
    assert set(keys) == set(query_params.keys()), """
    {}
    is different from expected:
    {}
    """.format(query_params.keys(), keys)
