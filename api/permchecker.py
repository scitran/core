# @author:  Renzo Frigato

from users import INTEGER_ROLES

def _get_access(container, uid):
    permissions_list = container['roles'] or container['permissions']
    for perm in permissions_list:
        if perm['_id'] == uid:
            return INTEGER_ROLES[perm['access']]
    else:
        return -1

def default_sublist(container, method, uid):
    access = _get_access(container, uid)

    if method == 'GET':
        return access >= INTEGER_ROLES['ro']
    if method in Set(['POST', 'PUT', 'DELETE']):
        return access >= INTEGER_ROLES['rw']
    return False

def group_roles_sublist(container, method, uid):
    access = _get_access(container, uid)
    return access >= INTEGER_ROLES['admin']

def always_true(container, method, uid):
    return True

