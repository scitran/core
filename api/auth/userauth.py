import logging
import sys

log = logging.getLogger('scitran.api')

from . import _get_access, always_ok, INTEGER_ROLES


def default(handler, user=None):
    def g(exec_op):
        def f(method, _id=None, query=None, payload=None, projection=None):
            if handler.public_request:
                handler.abort(403, 'public request is not authorized')
            elif method == 'PUT' and (handler.uid == _id or handler.superuser_request):
                if 'root' not in payload or payload['root'] == user['root']:
                    pass
                else:
                    handler.abort(400, 'user cannot alter own superuser privilege')
            elif method == 'POST' and not handler.superuser_request:
                handler.abort(403, 'only superuser are allowed to create users')
            elif method == 'POST' and handler.superuser_request:
                pass
            elif method == 'GET' and (handler.superuser_request or _id == handler.uid):
                pass
            elif method == 'DELETE' and handler.superuser_request and _id != handler.uid:
                pass
            else:
                handler.abort(403, 'not allowed to perform operation')
            return exec_op(method, _id=_id, query=query, payload=payload, projection=projection)
        return f
    return g

def list_permission_checker(handler):
    def g(exec_op):
        def f(method, query=None, projection=None):
            if handler.public_request:
                self.abort(403, 'public request is not authorized')
            return exec_op(method, query, projection)
        return f
    return g