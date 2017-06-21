"""
Purpose of this module is to define all the permissions checker decorators for the RefererHandler classes.
"""

from . import _get_access, INTEGER_ROLES


def default_referer(handler, parent_container=None):
    def g(exec_op):
        def f(method, _id, query_params=None, payload=None, exclude_params=None):
            access = _get_access(handler.uid, handler.user_site, parent_container)
            if method == 'GET' and parent_container.get('public', False):
                has_access = True
            elif method == 'GET':
                has_access = access >= INTEGER_ROLES['ro']
            elif method in ['POST', 'PUT', 'DELETE']:
                has_access = access >= INTEGER_ROLES['rw']
            else:
                has_access = False

            if has_access:
                return exec_op(method, _id, query_params, payload, exclude_params)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on parent container'.format(method))
        return f
    return g


def public_request(handler, parent_container=None):
    def g(exec_op):
        def f(method, _id=None, payload=None):
            if method == 'GET' and parent_container.get('public', False):
                return exec_op(method, _id, payload)
            else:
                handler.abort(403, 'not authorized to perform a {} operation on parent container'.format(method))
        return f
    return g
