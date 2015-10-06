# @author:  Renzo Frigato

def _get_access(uid, container):
    permissions_list = container.get('roles', container['permissions'])
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