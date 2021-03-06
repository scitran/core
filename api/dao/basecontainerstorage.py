import copy
import bson
import datetime
import pymongo.errors

from . import consistencychecker
from . import containerutil
from .. import config
from .. import util

from ..web.errors import APIStorageException, APIConflictException, APINotFoundException

log = config.log

# TODO: Find a better place to put this until OOP where we can just call cont.children
CHILD_MAP = {
    'groups':   'projects',
    'projects': 'sessions',
    'sessions': 'acquisitions'
}

PARENT_MAP = {v: k for k,v in CHILD_MAP.iteritems()}

# All "containers" are required to return these fields
# 'All' includes users
BASE_DEFAULTS = {
    '_id':      None,
    'created':  None,
    'modified': None
}

# All containers that inherit from 'container' in the DM
CONTAINER_DEFAULTS = BASE_DEFAULTS.copy()
CONTAINER_DEFAULTS.update({
    'permissions':  [],
    'files':        [],
    'notes':        [],
    'tags':         [],
    'info':         {}
})


class ContainerStorage(object):
    """
    This class provides access to mongodb collection elements (called containers).
    It is used by ContainerHandler istances for get, create, update and delete operations on containers.
    Examples: projects, sessions, acquisitions and collections
    """

    def __init__(self, cont_name, use_object_id=False, use_delete_tag=False, parent_cont_name=None, child_cont_name=None):
        self.cont_name = cont_name
        self.parent_cont_name = parent_cont_name
        self.child_cont_name = child_cont_name
        self.use_object_id = use_object_id
        self.use_delete_tag = use_delete_tag
        self.dbc = config.db[cont_name]

    @classmethod
    def factory(cls, cont_name):
        """
        Factory method to aid in the creation of a ContainerStorage instance
        when cont_name is dynamic.
        """
        cont_storage_name = containerutil.singularize(cont_name).capitalize() + 'Storage'
        for subclass in cls.__subclasses__():
            if subclass.__name__ == cont_storage_name:
                return subclass()
        return cls(containerutil.pluralize(cont_name))

    @classmethod
    def get_top_down_hierarchy(cls, cont_name, cid):
        parent_to_child = {
            'groups': 'projects',
            'projects': 'sessions',
            'sessions': 'acquisitions'
        }

        parent_tree = {
            cont_name: [cid]
        }
        parent_name = cont_name
        while parent_to_child.get(parent_name):
            # Parent storage
            storage = ContainerStorage.factory(parent_name)
            child_name = parent_to_child[parent_name]
            parent_tree[child_name] = []

            # For each parent id, find all of its children and add them to the list of child ids in the parent tree
            for parent_id in parent_tree[parent_name]:
                parent_tree[child_name] = parent_tree[child_name] + [cont["_id"] for cont in storage.get_children_legacy(parent_id, projection={'_id':1})]

            parent_name = child_name
        return parent_tree

    def _fill_default_values(self, cont):
        if cont:
            defaults = BASE_DEFAULTS.copy()
            if self.cont_name not in ['groups', 'users']:
                defaults = CONTAINER_DEFAULTS.copy()
            for k,v in defaults.iteritems():
                cont.setdefault(k, v)


    def get_container(self, _id, projection=None, get_children=False):
        cont = self.get_el(_id, projection=projection)
        if cont is None:
            raise APINotFoundException('Could not find {} {}'.format(self.cont_name, _id))
        if get_children:
            children = self.get_children(_id, projection=projection)
            cont[containerutil.pluralize(self.child_cont_name)] = children
        return cont

    def get_children_legacy(self, _id, projection=None, uid=None):
        """
        A get_children method that returns sessions from the project level rather than subjects.
        Will be removed when Subject completes it's transition to a stand alone collection.
        """
        try:
            child_name = CHILD_MAP[self.cont_name]
        except KeyError:
            raise APIStorageException('Children cannot be listed from the {0} level'.format(self.cont_name))
        if not self.use_object_id:
            query = {containerutil.singularize(self.cont_name): _id}
        else:
            query = {containerutil.singularize(self.cont_name): bson.ObjectId(_id)}

        if uid:
            query['permissions'] = {'$elemMatch': {'_id': uid}}
        if not projection:
            projection = {'info': 0, 'files.info': 0, 'subject': 0, 'tags': 0}
        return ContainerStorage.factory(child_name).get_all_el(query, None, projection)


    def get_children(self, _id, query=None, projection=None, uid=None):
        child_name = self.child_cont_name
        if not child_name:
            raise APIStorageException('Children cannot be listed from the {0} level'.format(self.cont_name))
        if not query:
            query = {}
        if not self.use_object_id:
            query[containerutil.singularize(self.cont_name)] = _id
        else:
            query[containerutil.singularize(self.cont_name)] = bson.ObjectId(_id)

        if uid:
            query['permissions'] = {'$elemMatch': {'_id': uid}}
        if not projection:
            projection = {'info': 0, 'files.info': 0, 'subject': 0, 'tags': 0}
        return ContainerStorage.factory(child_name).get_all_el(query, None, projection)


    def get_parent_tree(self, _id, cont=None, projection=None, add_self=False):
        parents = []

        curr_storage = self

        if not cont:
            cont = self.get_container(_id, projection=projection)

        if add_self:
            # Add the referenced container to the list
            cont['cont_type'] = self.cont_name
            parents.append(cont)

        # Walk up the hierarchy until we cannot go any further
        while True:

            try:
                parent = curr_storage.get_parent(cont['_id'], cont=cont, projection=projection)

            except (APINotFoundException, APIStorageException):
                # We got as far as we could, either we reached the top of the hierarchy or we hit a dead end with a missing parent
                break

            curr_storage = ContainerStorage.factory(curr_storage.parent_cont_name)
            parent['cont_type'] = curr_storage.cont_name
            parents.append(parent)

            if curr_storage.parent_cont_name:
                cont = parent
            else:
                break

        return parents

    def get_parent(self, _id, cont=None, projection=None):
        if not cont:
            cont = self.get_container(_id, projection=projection)

        if self.parent_cont_name:
            ps = ContainerStorage.factory(self.parent_cont_name)
            parent = ps.get_container(cont[self.parent_cont_name], projection=projection)
            return parent

        else:
            raise APIStorageException('The container level {} has no parent.'.format(self.cont_name))


    def _from_mongo(self, cont):
        pass

    def _to_mongo(self, payload):
        pass

    def exec_op(self, action, _id=None, payload=None, query=None, user=None,
                public=False, projection=None, recursive=False, r_payload=None,  # pylint: disable=unused-argument
                replace_metadata=False, unset_payload=None):
        """
        Generic method to exec a CRUD operation from a REST verb.
        """

        check = consistencychecker.get_container_storage_checker(action, self.cont_name)
        data_op = payload or {'_id': _id}
        check(data_op)
        if action == 'GET' and _id:
            return self.get_el(_id, projection=projection, fill_defaults=True)
        if action == 'GET':
            return self.get_all_el(query, user, projection, fill_defaults=True)
        if action == 'DELETE':
            return self.delete_el(_id)
        if action == 'PUT':
            return self.update_el(_id, payload, unset_payload=unset_payload, recursive=recursive, r_payload=r_payload, replace_metadata=replace_metadata)
        if action == 'POST':
            return self.create_el(payload)
        raise ValueError('action should be one of GET, POST, PUT, DELETE')

    def create_el(self, payload):
        self._to_mongo(payload)
        try:
            result = self.dbc.insert_one(payload)
        except pymongo.errors.DuplicateKeyError:
            raise APIConflictException('Object with id {} already exists.'.format(payload['_id']))
        return result

    def update_el(self, _id, payload, unset_payload=None, recursive=False, r_payload=None, replace_metadata=False):
        replace = None
        if replace_metadata:
            replace = {}
            if payload.get('info') is not None:
                replace['info'] = util.mongo_sanitize_fields(payload.pop('info'))
            if payload.get('subject') is not None and payload['subject'].get('info') is not None:
                replace['subject.info'] = util.mongo_sanitize_fields(payload['subject'].pop('info'))

        update = {}

        if payload is not None:
            self._to_mongo(payload)
            update['$set'] = util.mongo_dict(payload)

        if unset_payload is not None:
            update['$unset'] = util.mongo_dict(unset_payload)

        if replace is not None:
            update['$set'].update(replace)

        if self.use_object_id:
            try:
                _id = bson.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        if recursive and r_payload is not None:
            containerutil.propagate_changes(self.cont_name, _id, {}, {'$set': util.mongo_dict(r_payload)})
        return self.dbc.update_one({'_id': _id}, update)

    def delete_el(self, _id):
        if self.use_object_id:
            try:
                _id = bson.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        if self.use_delete_tag:
            return self.dbc.update_one({'_id': _id}, {'$set': {'deleted': datetime.datetime.utcnow()}})
        return self.dbc.delete_one({'_id':_id})

    def get_el(self, _id, projection=None, fill_defaults=False):
        if self.use_object_id:
            try:
                _id = bson.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        cont = self.dbc.find_one({'_id': _id, 'deleted': {'$exists': False}}, projection)
        self._from_mongo(cont)
        if fill_defaults:
            self._fill_default_values(cont)
        if cont is not None and cont.get('files', []):
            cont['files'] = [f for f in cont['files'] if 'deleted' not in f]
        return cont

    def get_all_el(self, query, user, projection, fill_defaults=False):
        if query is None:
            query = {}
        if user:
            if query.get('permissions'):
                query['$and'] = [{'permissions': {'$elemMatch': user}}, {'permissions': query.pop('permissions')}]
            else:
                query['permissions'] = {'$elemMatch': user}
        query['deleted'] = {'$exists': False}

        # if projection includes files.info, add new key `info_exists` and allow reserved info keys through
        if projection and ('info' in projection or 'files.info' in projection or 'subject.info' in projection):
            projection = copy.deepcopy(projection)
            replace_info_with_bool = True
            projection.pop('subject.info', None)
            projection.pop('files.info', None)
            projection.pop('info', None)

            # Replace with None if empty (empty projections only return ids)
            if not projection:
                projection = None
        else:
            replace_info_with_bool = False

        results = list(self.dbc.find(query, projection))
        for cont in results:
            if cont.get('files', []):
                cont['files'] = [f for f in cont['files'] if 'deleted' not in f]
            self._from_mongo(cont)
            if fill_defaults:
                self._fill_default_values(cont)

            if replace_info_with_bool:
                info = cont.pop('info', {})
                cont['info_exists'] = bool(info)
                cont['info'] = containerutil.sanitize_info(info)

                if cont.get('subject'):
                    s_info = cont['subject'].pop('info', {})
                    cont['subject']['info_exists'] = bool(s_info)
                    cont['subject']['info'] = containerutil.sanitize_info(s_info)

                for f in cont.get('files', []):
                    f_info = f.pop('info', {})
                    f['info_exists'] = bool(f_info)
                    f['info'] = containerutil.sanitize_info(f_info)

        return results

    def modify_info(self, _id, payload, modify_subject=False):

        # Support modification of subject info
        # Can be removed when subject becomes a standalone container
        info_key = 'subject.info' if modify_subject else 'info'

        update = {}
        set_payload = payload.get('set')
        delete_payload = payload.get('delete')
        replace_payload = payload.get('replace')

        if (set_payload or delete_payload) and replace_payload is not None:
            raise APIStorageException('Cannot set or delete AND replace info fields.')

        if replace_payload is not None:
            update = {
                '$set': {
                    info_key: util.mongo_sanitize_fields(replace_payload)
                }
            }

        else:
            if set_payload:
                update['$set'] = {}
                for k,v in set_payload.items():
                    update['$set'][info_key + '.' + k] = util.mongo_sanitize_fields(v)
            if delete_payload:
                update['$unset'] = {}
                for k in delete_payload:
                    update['$unset'][info_key + '.' + k] = ''

        if self.use_object_id:
            _id = bson.objectid.ObjectId(_id)
        query = {'_id': _id }

        if not update.get('$set'):
            update['$set'] = {'modified': datetime.datetime.utcnow()}
        else:
            update['$set']['modified'] = datetime.datetime.utcnow()

        return self.dbc.update_one(query, update)
