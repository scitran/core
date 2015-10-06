# @author:  Renzo Frigato

import datetime
import logging

import json

import copy
import os

from .. import validators
from ..auth import containerauth, always_ok
from .. import files
from ..dao import containerstorage
from .. import base
from .. import util

log = logging.getLogger('scitran.api')




class ContainerHandler(base.RequestHandler):
    """
    This class handle operations on a generic container

    The pattern used is:
    1) initialize request
    2) exec request
    3) check and return result

    Specific behaviors (permissions checking logic for authenticated and not superuser users, storage interaction)
    are specified in the container_handler_configurations
    """

    default_list_projection = ['files', 'notes', 'timestamp', 'timezone', 'public']

    container_handler_configurations = {
        'projects': {
            'storage': containerstorage.CollectionStorage('projects', use_oid=True),
            'permchecker': containerauth.default_container,
            'parent_storage': containerstorage.CollectionStorage('groups', use_oid=False),
            'mongo_schema_file': 'mongo/project.json',
            'payload_schema_file': 'input/project.json',
            'list_projection': ['label', 'subject.code', 'project', 'group'] + default_list_projection
        },
        'sessions': {
            'storage': containerstorage.CollectionStorage('sessions', use_oid=True),
            'permchecker': containerauth.default_container,
            'parent_storage': containerstorage.CollectionStorage('projects', use_oid=True),
            'mongo_schema_file': 'mongo/session.json',
            'payload_schema_file': 'input/session.json',
            'list_projection': ['label', 'subject.code', 'project', 'group'] + default_list_projection
        },
        'acquisitions': {
            'storage': containerstorage.CollectionStorage('acquisitions', use_oid=True),
            'permchecker': containerauth.default_container,
            'parent_storage': containerstorage.CollectionStorage('sessions', use_oid=True),
            'mongo_schema_file': 'mongo/acquisitions.json',
            'payload_schema_file': 'input/acquisitions.json',
            'list_projection': ['label', 'subject.code', 'project', 'group'] + default_list_projection
        },
        'collections': {
            'permchecker': containerauth.collection_permissions,
            'parent_storage': containerstorage.CollectionStorage('collections', use_oid=True),
            'mongo_schema_file': 'mongo/collections.json',
            'payload_schema_file': 'input/collections.json'
        }
    }


    def __init__(self, request=None, response=None):
        super(ContainerHandler, self).__init__(request, response)

    def get(self, coll_name, **kwargs):
        _id = kwargs.pop('cid')
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        container= self._get_container(_id)
        permchecker = self._get_permchecker(container)

        result = permchecker(self.storage.exec_op)('GET', _id)

        if result is None:
            self.abort(404, 'Element not found in collection {} {}'.format(storage.coll_name, _id))
        return result

    def get_all(self, par_coll_name, coll_name, par_id):
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        public = self.request.GET.get('public', '').lower() in ('1', 'true')
        projection = {p: 1 for p in self.config['list_projection']}
        projection['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site or self.app.config['site_id']}}
        if self.superuser_request:
            permchecker = always_ok
        elif self.public_request:
            public = True
            permchecker = always_ok
        else:
            admin_only = self.request.GET.get('admin', '').lower() in ('1', 'true')
            permchecker = containerauth.list_permission_checker(self, admin_only)
        query = {par_coll_name[:-1]: par_id}
        result = permchecker(self.storage.exec_op)('GET', query=query, public=public, projection=projection)
        if result is None:
            self.abort(404, 'Element not found in collection {} {}'.format(storage.coll_name, _id))
        return result

    def get_all_for_user(self, coll_name, uid):
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        projection = {p: 1 for p in self.config['list_projection']}
        projection['permissions'] = {'$elemMatch': {'_id': uid, 'site': self.app.config['site_id']}}
        if self.superuser_request:
            permchecker = always_ok
        elif self.public_request:
            self.abort(403, 'this request is not allowed')
        else:
            permchecker = containerauth.list_permission_checker(self)
        query = {}
        user = {
            '_id': uid,
            'site': self.app.config['site_id']
        }
        result = permchecker(self.storage.exec_op)('GET', query=query, user=user, projection=projection)
        if result is None:
            self.abort(404, 'Element not found in collection {} {}'.format(storage.coll_name, _id))
        return result

    def post(self, coll_name, **kwargs):
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body
        log.debug(payload)
        payload_validator(payload, 'POST')
        parent_container = self._get_parent_container(payload)
        payload['permissions'] = parent_container.get('roles')
        if payload['permissions'] is None:
            payload['permissions'] = parent_container['permissions']
        permchecker = self._get_permchecker(parent_container=parent_container)
        result = mongo_validator(permchecker(self.storage.exec_op))('POST', payload=payload)

        if result.acknowledged:
            return {'_id': result.inserted_id}
        else:
            self.abort(404, 'Element not added in collection {} {}'.format(storage.coll_name, _id))

    def put(self, coll_name, **kwargs):
        _id = kwargs.pop('cid')
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        container = self._get_container(_id)
        mongo_validator, payload_validator = self._get_validators()

        payload = self.request.json_body
        payload_validator(payload, 'PUT')

        target_parent_container = self._get_parent_container(payload)
        if target_parent_container:
            payload['permissions'] = target_parent_container.get('roles')
            if payload['permissions'] is None:
                payload['permissions'] = target_parent_container['permissions']

        permchecker = self._get_permchecker(container, target_parent_container)
        result = mongo_validator(permchecker(self.storage.exec_op))('PUT', _id=_id, payload=payload)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not updated in collection {} {}'.format(storage.coll_name, _id))

    def delete(self, coll_name, **kwargs):
        _id = kwargs.pop('cid')
        self.config = self.container_handler_configurations[coll_name]
        self._init_storage()
        container= self._get_container(_id)
        parent_container = self._get_parent_container(container)
        permchecker = self._get_permchecker(container, parent_container)

        result = permchecker(self.storage.exec_op)('DELETE', _id)

        if result.deleted_count == 1:
            return {'deleted': result.deleted_count}
        else:
            self.abort(404, 'Element not removed from collection {} {}'.format(storage.coll_name, _id))

    def _get_validators(self):
        mongo_validator = validators.mongo_from_schema_file(self, self.config.get('mongo_schema_file'))
        payload_validator = validators.payload_from_schema_file(self, self.config.get('payload_schema_file'))
        return mongo_validator, payload_validator

    def _get_parent_container(self, payload):
        if self.config.get('parent_storage'):
            return None
        log.debug(payload)
        parent_storage = self.config['parent_storage']
        parent_id_property = parent_storage.coll_name[:-1]
        log.debug(parent_id_property)
        parent_id = payload.get(parent_id_property)
        log.debug(parent_id)
        if parent_id:
            parent_storage.dbc = self.app.db[parent_storage.coll_name]
            parent_container = parent_storage.get_container(parent_id)
            if parent_container is None:
                self.abort(404, 'Element {} not found in collection {}'.format(parent_id, parent_storage.coll_name))
        else:
            parent_container = None
        log.debug(parent_container)
        return parent_container

    def _init_storage(self):
        self.storage = self.config['storage']
        self.storage.dbc = self.app.db[self.storage.coll_name]

    def _get_container(self, _id):
        container = self.storage.get_container(_id)
        if container is not None:
            return container
        else:
            self.abort(404, 'Element {} not found in collection {}'.format(_id, self.storage.coll_name))

    def _get_permchecker(self, container=None, parent_container=None):
        if self.superuser_request:
            return always_ok
        elif self.public_request:
            return containerauth.public_request(self, container, parent_container)
        else:
            permchecker = self.config['permchecker']
            return permchecker(self, container, parent_container)
