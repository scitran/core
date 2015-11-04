# @author:  Renzo Frigato
ROLES = [
    {
        'rid': 'ro',
        'name': 'Read-Only',
    },
    {
        'rid': 'rw',
        'name': 'Read-Write',
    },
    {
        'rid': 'admin',
        'name': 'Admin',
    },
]

INTEGER_ROLES = {r['rid']: i for i, r in enumerate(ROLES)}

def _get_access(uid, site, container):
    permissions_list = container.get('roles', container.get('permissions', []))
    for perm in permissions_list:
        if perm['_id'] == uid and perm['site'] == site:
            return INTEGER_ROLES[perm['access']]
    else:
        return -1

def always_ok(exec_op):
    """
    This decorator leaves the original method unchanged.
    It is used as permissions checker when the request is a superuser_request
    """
    return exec_op
