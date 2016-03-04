from .. import config
from . import APIConsistencyException

log = config.log

def noop(*args, **kwargs):
    pass

def get_list_storage_checker(action, list_name):
    if list_name == 'permissions' and action == 'POST':
        return user_on_permissions
    return noop

def get_container_storage_checker(action, cont_name):
    if cont_name == 'projects' and action in ['PUT', 'POST']:
        return field_on_container('session', 'sessions')
    elif cont_name == 'sessions' and action in ['PUT', 'POST']:
        return field_on_container('project', 'projects')
    elif cont_name == 'acquisitions' and action in ['PUT', 'POST']:
        return field_on_container('acquisition', 'acquisitions')
    elif cont_name == 'projects' and action == 'DELETE':
        return check_children('project', 'sessions')
    elif cont_name == 'sessions' and action == 'DELETE':
        return check_children('session', 'acquisitions')
    return noop

def user_on_permission(payload, **kwargs):
    if payload['site'] == config.get_item('site', '_id'):
        if not config.db.users.find_one({'_id': payload['_id']}):
            raise APIConsistencyException('user does not exist')

def field_on_container(field, container_name):
    def f(payload, **kwargs):
        if payload.get(field) and not config.db[container_name].find_one({'_id': payload[field]}):
            raise APIConsistencyException('{} {} does not exist'.format(field, payload[field]))
    return f

def check_children(foreign_key_field, container_name):
    def f(payload, **kwargs):
        if config.db[container_name].find_one({foreign_key_field, payload['_id']}):
            raise APIConsistencyException(
                'DELETE not allowed. Children {} found for {}'.format(container_name, payload['_id'])
            )
    return f