# @author:  Renzo Frigato

from users import INTEGER_ROLES

def _get_access(uid, container):
    permissions_list = container.get('roles') or container.get('permissions')
    for perm in permissions_list:
        if perm['_id'] == uid:
            return INTEGER_ROLES[perm['access']]
    else:
        return -1


def always_ok(apply_change):
    return apply_change


def default_sublist(handler, container):
    access = _get_access(handler.uid, container)
    def g(apply_change):
        def f(method, _id, elem_match = None, payload = None):
            if method == 'GET' and container.get('public', False):
                min_access = -1
            elif method == 'GET':
                min_access = INTEGER_ROLES['ro']
            elif method in Set(['POST', 'PUT', 'DELETE']):
                min_access = INTEGER_ROLES['rw']
            else:
                min_access = float('inf')

            if access >= min_access:
                return apply_change(method, _id, elem_match, payload)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
        return f
    return g


def group_roles_sublist(handler, container):
    access = _get_access(handler.uid, container)
    def g(apply_change):
        def f(method, _id, elem_match = None, payload = None):
            if method == 'GET' and elem_match.get('_id') == handler.uid:
                return apply_change(method, _id, elem_match, payload)
            elif access >= INTEGER_ROLES['admin']:
                return apply_change(method, _id, elem_match, payload)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
        return f
    return g

def public_request(handler, container):
    def g(apply_change):
        def f(method, _id, elem_match = None, payload = None):
            if method == 'GET' and container.get('public', False):
                return apply_change(method, _id, elem_match, payload)
            else:
                handler.abort(403, 'not authorized to perform a {} operation on this container'.format(method))
        return f
    return g