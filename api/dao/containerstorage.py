import bson.errors
import bson.objectid
import json
import pymongo.errors

from .. import util
from .. import config
from . import consistencychecker
from . import APIStorageException, APIConflictException, APINotFoundException
from . import hierarchy

log = config.log

# TODO: Find a better place to put this until OOP where we can just call cont.children
CHILD_MAP = {
    'groups':   'projects',
    'projects': 'sessions',
    'sessions': 'acquisitions'
}

class ContainerStorage(object):
    """
    This class provides access to mongodb collection elements (called containers).
    It is used by ContainerHandler istances for get, create, update and delete operations on containers.
    Examples: projects, sessions, acquisitions and collections
    """

    def __init__(self, cont_name, use_object_id = False):
        self.cont_name = cont_name
        self.use_object_id = use_object_id
        self.dbc = config.db[cont_name]


    @staticmethod
    def factory(cont_name, use_object_id = False):
        """
        Factory method to aid in the creation of a ContainerStorage instance
        when cont_name is dynamic.
        """
        if cont_name == 'groups':
            return GroupStorage()
        elif cont_name == 'projects':
            return ProjectStorage()
        elif cont_name == 'sessions':
            return SessionStorage()
        elif cont_name == 'acquisitions':
            return AcquisitionStorage()
        else:
            return ContainerStorage(cont_name, use_object_id)

    def get_container(self, _id, projection=None, get_children=False):
        cont = self.get_el(_id, projection=projection)
        if get_children:
            children = self.get_children(_id, projection=projection)
            cont[CHILD_MAP.get(self.cont_name)] = children
        return cont

    def get_children(self, _id, projection=None):
        child_name = CHILD_MAP.get(self.cont_name)
        if not child_name:
            raise ValueError('Children can only be listed from group, project or session level')
        query = {self.cont_name[:-1]: bson.objectid.ObjectId(_id)}
        return self.factory(child_name, use_object_id=True).get_all_el(query, None, projection)


    def exec_op(self, action, _id=None, payload=None, query=None, user=None,
                public=False, projection=None, recursive=False, r_payload=None,  # pylint: disable=unused-argument
                replace_metadata=False):
        """
        Generic method to exec a CRUD operation from a REST verb.
        """

        check = consistencychecker.get_container_storage_checker(action, self.cont_name)
        data_op = payload or {'_id': _id}
        check(data_op)
        if action == 'GET' and _id:
            return self.get_el(_id, projection)
        if action == 'GET':
            return self.get_all_el(query, user, projection)
        if action == 'DELETE':
            return self.delete_el(_id)
        if action == 'PUT':
            return self.update_el(_id, payload, recursive=recursive, r_payload=r_payload, replace_metadata=replace_metadata)
        if action == 'POST':
            return self.create_el(payload)
        raise ValueError('action should be one of GET, POST, PUT, DELETE')

    def create_el(self, payload):
        log.debug(payload)
        try:
            result = self.dbc.insert_one(payload)
        except pymongo.errors.DuplicateKeyError:
            raise APIConflictException('Object with id {} already exists.'.format(payload['_id']))
        return result

    def update_el(self, _id, payload, recursive=False, r_payload=None, replace_metadata=False):
        replace = None
        if replace_metadata:
            replace = {}
            if payload.get('metadata') is not None:
                replace['metadata'] = util.mongo_sanitize_fields(payload.pop('metadata'))
            if payload.get('subject') is not None and payload['subject'].get('metadata') is not None:
                replace['subject.metadata'] = util.mongo_sanitize_fields(payload['subject'].pop('metadata'))

        update = {
            '$set': util.mongo_dict(payload)
        }
        if replace is not None:
            update['$set'].update(replace)

        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        if recursive and r_payload is not None:
            hierarchy.propagate_changes(self.cont_name, _id, {}, {'$set': util.mongo_dict(r_payload)})
        return self.dbc.update_one({'_id': _id}, update)

    def delete_el(self, _id):
        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.delete_one({'_id':_id})

    def get_el(self, _id, projection=None):
        if self.use_object_id:
            try:
                _id = bson.objectid.ObjectId(_id)
            except bson.errors.InvalidId as e:
                raise APIStorageException(e.message)
        return self.dbc.find_one(_id, projection)

    def get_all_el(self, query, user, projection):
        if user:
            if query.get('permissions'):
                query['$and'] = [{'permissions': {'$elemMatch': user}}, {'permissions': query.pop('permissions')}]
            else:
                query['permissions'] = {'$elemMatch': user}
        log.debug(query)
        log.debug(projection)
        result = self.dbc.find(query, projection)
        return list(result)

class GroupStorage(ContainerStorage):

    def __init__(self):
        super(GroupStorage,self).__init__('groups', use_object_id=False)

    def create_el(self, payload):
        log.debug(payload)
        roles = payload.pop('roles')
        return self.dbc.update_one(
            {'_id': payload['_id']},
            {
                '$set': payload,
                '$setOnInsert': {'roles': roles}
            },
            upsert=True)


class ProjectStorage(ContainerStorage):

    def __init__(self):
        super(ProjectStorage,self).__init__('projects', use_object_id=True)

    def recalc_sessions_compliance(self, project_id=None):
        if project_id is None:
            # Recalc all projects
            projects = self.get_all_el({'template': {'$exists': True}}, None, None)
        else:
            project = self.get_container(project_id, get_children=True)
            if project:
                projects = [project]
            else:
                raise APINotFoundException('Could not find project {}'.format(project_id))
        changed_sessions = []

        for project in projects:
            template = json.loads(project.get('template',{}))
            if not template:
                return
            else:
                session_storage = SessionStorage()
                for s in project.get('sessions', []):
                    changed = session_storage.recalc_session_compliance(s['_id'], session=s, template=template)
                    if changed:
                        changed_sessions.append(s['_id'])
        return changed_sessions


class SessionStorage(ContainerStorage):

    def __init__(self):
        super(SessionStorage,self).__init__('sessions', use_object_id=True)

    def create_el(self, payload):
        project = ContainerStorage('projects', use_object_id=True).get_container(payload['project'])
        if project.get('template'):
            payload['project_has_template'] = True
            payload['satisfies_template'] = hierarchy.is_session_compliant(payload, project.get('template'))
        return super(SessionStorage, self).create_el(payload)

    def update_el(self, _id, payload, recursive=False, r_payload=None, replace_metadata=False):
        session = self.get_container(_id)
        if session is None:
            raise APINotFoundException('Could not find session {}'.format(_id))
        if session.get('project_has_template') or payload.get('project_has_template'):
            project = ContainerStorage('projects', use_object_id=True).get_container(session['project'])
            session.update(payload)
            payload['satisfies_template'] = hierarchy.is_session_compliant(session, json.loads(project.get('template')))
        return super(SessionStorage, self).update_el(_id, payload, recursive, r_payload, replace_metadata)

    def recalc_session_compliance(self, session_id, session=None, template=None):
        """
        Calculates a session's compliance with the project's session template.
        Returns True if the status changed, False otherwise
        """
        if session is None:
            session = self.get_container(session_id)
        if session is None:
            raise APINotFoundException('Could not find session {}'.format(session_id))
        if session.get('project_has_template'):
            if template is None:
                template = json.loads(ContainerStorage('projects', use_object_id=True).get_container(session['project']).get('template'))
            satisfies_template = hierarchy.is_session_compliant(session, template)
            if session.get('satisfies_template') != satisfies_template:
                update = {'satisfies_template': satisfies_template}
                super(SessionStorage, self).update_el(session_id, update)
                return True
        return False


class AcquisitionStorage(ContainerStorage):

    def __init__(self):
        super(AcquisitionStorage,self).__init__('acquisitions', use_object_id=True)

    def create_el(self, payload):
        result = super(AcquisitionStorage, self).create_el(payload)
        SessionStorage().recalc_session_compliance(payload['session'])
        return result

    def update_el(self, _id, payload, recursive=False, r_payload=None, replace_metadata=False):
        result = super(AcquisitionStorage, self).update_el(_id, payload, recursive, r_payload, replace_metadata)
        acquisition = self.get_container(_id)
        if acquisition is None:
            raise APINotFoundException('Could not find acquisition {}'.format(_id))
        SessionStorage().recalc_session_compliance(acquisition['session'])
        return result

    def delete_el(self, _id):
        acquisition = self.get_container(_id)
        if acquisition is None:
            raise APINotFoundException('Could not find acquisition {}'.format(_id))
        result = super(AcquisitionStorage, self).delete_el(_id)
        SessionStorage().recalc_session_compliance(acquisition['session'])
        return result

