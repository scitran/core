# @author:  Renzo Frigato
"""
Purpose of this module is to define all the permissions checker decorators.

This decorators are currently supported only by the ListHandler and FileListHandler classes.
"""
import logging
import sys

from users import INTEGER_ROLES

log = logging.getLogger('scitran.api')

def _get_access(uid, container):
    permissions_list = container.get('roles') or container.get('permissions')
    for perm in permissions_list:
        if perm['_id'] == uid:
            return INTEGER_ROLES[perm['access']]
    else:
        return -1

def always_ok(exec_op):
    """
    This decorator leaves the original method unchanged.
    It is used as permissions checker when the request is a superuser_request
    """
    return exec_op

def default_sublist(handler, container):
    """
    This is the default permissions checker generator.
    The resulting permissions checker modifies the exec_op method by checking the user permissions
    on the container before actually executing this method.
    """
    access = _get_access(handler.uid, container)
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None):
            if method == 'GET' and container.get('public', False):
                min_access = -1
            elif method == 'GET':
                min_access = INTEGER_ROLES['ro']
            elif method in ['POST', 'PUT', 'DELETE']:
                min_access = INTEGER_ROLES['rw']
            else:
                min_access = sys.maxint

            if access >= min_access:
                return exec_op(method, _id, query_params, payload)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
        return f
    return g

def group_roles_sublist(handler, container):
    """
    This is the customized permissions checker for group roles operations.
    """
    access = _get_access(handler.uid, container)
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None):
            if method in ['GET', 'DELETE']  and query_params.get('_id') == handler.uid:
                return exec_op(method, _id, query_params, payload)
            elif access >= INTEGER_ROLES['admin']:
                return exec_op(method, _id, query_params, payload)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
        return f
    return g

def permissions_sublist(handler, container):
    """
    the customized permissions checker for permissions operations.
    """
    access = _get_access(handler.uid, container)
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None):
            log.error(query_params)
            if method in ['GET', 'DELETE']  and query_params.get('_id') == handler.uid and query_params.get('site') == (handler.source_site or handler.app.config['site_id']):
                return exec_op(method, _id, query_params, payload)
            elif access >= INTEGER_ROLES['admin']:
                return exec_op(method, _id, query_params, payload)
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
        return f
    return g

def notes_sublist(handler, container):
    """
    permissions checker for notes_sublist
    """
    access = _get_access(handler.uid, container)
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None):
            if access >= INTEGER_ROLES['admin']:
                pass
            elif method == 'POST' and access >= INTEGER_ROLES['rw'] and payload['author'] == handler.uid:
                pass
            elif method == 'GET' and (access >= INTEGER_ROLES['ro'] or container.get('public')):
                pass
            elif method in ['GET', 'DELETE', 'PUT'] and container['notes'][0]['author'] == handler.uid:
                pass
            else:
                handler.abort(403, 'user not authorized to perform a {} operation on the list'.format(method))
            return exec_op(method, _id, query_params, payload)
        return f
    return g

def public_request(handler, container):
    """
    For public requests we allow only GET operations on containers marked as public.
    """
    def g(exec_op):
        def f(method, _id, query_params = None, payload = None):
            if method == 'GET' and container.get('public', False):
                return exec_op(method, _id, query_params, payload)
            else:
                handler.abort(403, 'not authorized to perform a {} operation on this container'.format(method))
        return f
    return g
