import bson
from ..web import base
from .. import config, validators
from ..auth import require_login
from ..dao.containerstorage import SearchStorage

from ..auth import groupauth
from ..dao import noop


log = config.log
storage = SearchStorage()

class SavedSearchHandler(base.RequestHandler):

	def __init__(self, request=None, response=None):
		super(SavedSearchHandler, self).__init__(request, response)

	@require_login
	def post(self):
		payload = self.request.json_body
		validators.validate_data(payload, 'search-input.json', 'input', 'POST')
		payload['permissions'] = [{"_id": self.uid, "access": "admin"}]
		result = storage.create_el(payload)	
		if result.acknowledged:
			if result.inserted_id:
				return {'_id': result.inserted_id}
		return {"hi" : "bye"}

	def get_all(self):
		log.debug(self.uid)
		return storage.get_all_el({}, {'_id': self.uid}, {'label': 1})

	def get(self, sid):
		result = storage.get_el(sid)
		if result is None:
			self.abort(404, 'Element {} not found'.format(sid))
		return result

	def delete(self, sid):
		search = storage.get_container(sid)
		permchecker = groupauth.default(self, search)
		result = permchecker(storage.exec_op)('DELETE', sid)
		if result.deleted_count == 1:
			return {'deleted': result.deleted_count}
		else:
			self.abort(404, 'Group {} not removed'.format(sid))
		return result

	def replace_search(self, sid):
		payload = self.request.json_body
		if payload.get('_id'):
			del(payload['_id'])
		if payload.get('permissions'):
			del(payload['permissions'])
		validators.validate_data(payload, 'search-input.json', 'input', 'POST')
		payload['_id'] = bson.ObjectId(sid)
		search = storage.get_container(sid)
		payload['permissions'] = search['permissions']
		permchecker = groupauth.default(self, search)
		permchecker(noop)('DELETE', sid)
		result = storage.replace_el(payload)
		if result.acknowledged:
			if result.inserted_id:
				return {'_id': result.inserted_id}
		return {"hi" : "bye"}
