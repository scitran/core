import bson
import datetime

from .. import config
from .. import validators
from ..auth import containerauth, always_ok, has_access
from ..dao import APIPermissionException, APINotFoundException
from ..dao.containerstorage import ProjectStorage
from ..jobs.gears import get_gear_by_name
from ..validators import validate_data, verify_payload_exists
from ..web import base



log = config.log

class ProjectSettings(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ProjectSettings, self).__init__(request, response)
        self.storage = ProjectStorage()

    def set_project_template(self, **kwargs):
        project_id = kwargs.pop('cid')
        container = self.storage.get_container(project_id)

        template = self.request.json_body
        validators.validate_data(template, 'project-template.json', 'input', 'POST')
        payload = {'template': template}
        payload['modified'] = datetime.datetime.utcnow()

        permchecker = self._get_permchecker(container)
        result = permchecker(self.storage.exec_op)('PUT', _id=project_id, payload=payload)
        return {'modified': result.modified_count}

    def delete_project_template(self, **kwargs):
        project_id = kwargs.pop('cid')
        container = self.storage.get_container(project_id)

        payload = {'modified': datetime.datetime.utcnow()}
        unset_payload = {'template': ''}

        permchecker = self._get_permchecker(container)
        result = permchecker(self.storage.exec_op)('PUT', _id=project_id, payload=payload, unset_payload=unset_payload)
        return {'modified': result.modified_count}

    def get_phi(self, cid):
        projection = None

        if cid == 'site':
            if self.public_request:
                raise APIPermissionException('Viewing site-level PHI fields requires login.')
            projection = {'project_id': 0}
        else:
            project = self.storage.get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'ro'):
                raise APIPermissionException('User does not have access to project {} PHI fields'.format(cid))

        return self.storage.get_phi_fields(cid, projection=projection)

    def update_phi(self, cid):

        if cid == 'site':
            if not self.user_is_admin:
                raise APIPermissionException('Modifying site-level PHI fields can only be done by a site admin.')
        else:
            project = self.storage.get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'admin'):
                raise APIPermissionException('User does not have access to project {} PHI fields'.format(cid))

        result = self.storage.add_phi_fields(cid, self.request.json_body)
        log.debug(result)
        if result['nModified'] == 1 or result.get('upserted'):
            return {"modified": result['nModified']}
        else:
            self.abort(404, "Unable to update phi fields for project_id:{}".format(cid))

    def _get_permchecker(self, container=None, parent_container=None):
        if self.superuser_request:
            return always_ok
        elif self.public_request:
            return containerauth.public_request(self, container)
        else:
            permchecker = containerauth.default_container
            return permchecker(self, container, parent_container)


class RulesHandler(base.RequestHandler):

    def get(self, cid):
        """List rules"""

        projection = None

        if cid == 'site':
            if self.public_request:
                raise APIPermissionException('Viewing site-level rules requires login.')
            projection = {'project_id': 0}
        else:
            project = ProjectStorage().get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'ro'):
                raise APIPermissionException('User does not have access to project {} rules'.format(cid))

        return config.db.project_rules.find({'project_id' : cid}, projection=projection)


    @verify_payload_exists
    def post(self, cid):
        """Add a rule"""

        if cid == 'site':
            if not self.user_is_admin:
                raise APIPermissionException('Adding site-level rules can only be done by a site admin.')
        else:
            project = ProjectStorage().get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'admin'):
                raise APIPermissionException('Adding rules to a project can only be done by a project admin.')

        doc = self.request.json

        validate_data(doc, 'rule-new.json', 'input', 'POST', optional=True)
        validate_regexes(doc)
        try:
            get_gear_by_name(doc['alg'])
        except APINotFoundException:
            self.abort(400, 'Cannot find gear for alg {}, alg not valid'.format(doc['alg']))

        doc['project_id'] = cid

        result = config.db.project_rules.insert_one(doc)
        return { '_id': result.inserted_id }

class RuleHandler(base.RequestHandler):

    def get(self, cid, rid):
        """Get rule"""

        projection = None
        if cid == 'site':
            if self.public_request:
                raise APIPermissionException('Viewing site-level rules requires login.')
            projection = {'project_id': 0}
        else:
            project = ProjectStorage().get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'ro'):
                raise APIPermissionException('User does not have access to project {} rules'.format(cid))

        result = config.db.project_rules.find_one({'project_id' : cid, '_id': bson.ObjectId(rid)}, projection=projection)

        if not result:
            raise APINotFoundException('Rule not found.')

        return result


    @verify_payload_exists
    def put(self, cid, rid):
        """Change a rule"""

        if cid == 'site':
            if not self.user_is_admin:
                raise APIPermissionException('Modifying site-level rules can only be done by a site admin.')
        else:
            project = ProjectStorage().get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'admin'):
                raise APIPermissionException('Modifying project rules can only be done by a project admin.')

        doc = config.db.project_rules.find_one({'project_id' : cid, '_id': bson.ObjectId(rid)})

        if not doc:
            raise APINotFoundException('Rule not found.')

        updates = self.request.json
        validate_data(updates, 'rule-update.json', 'input', 'POST', optional=True)
        validate_regexes(updates)
        if updates.get('alg'):
            try:
                get_gear_by_name(updates['alg'])
            except APINotFoundException:
                self.abort(400, 'Cannot find gear for alg {}, alg not valid'.format(updates['alg']))

        doc.update(updates)
        config.db.project_rules.replace_one({'_id': bson.ObjectId(rid)}, doc)

        return


    def delete(self, cid, rid):
        """Remove a rule"""

        if cid == 'site':
            if not self.user_is_admin:
                raise APIPermissionException('Modifying site-level rules can only be done by a site admin.')
        else:
            project = ProjectStorage().get_container(cid, projection={'permissions': 1})
            if not self.user_is_admin and not has_access(self.uid, project, 'admin'):
                raise APIPermissionException('Modifying project rules can only be done by a project admin.')


        result = config.db.project_rules.delete_one({'project_id' : cid, '_id': bson.ObjectId(rid)})
        if result.deleted_count != 1:
            raise APINotFoundException('Rule not found.')
        return
