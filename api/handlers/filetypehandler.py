from ..web import base
from .. import config
from .. import validators
from ..auth import userauth
from ..dao import noop

class FileType(base.RequestHandler):

    def get(self):
        """Get file types"""
        resp = config.db.filetypes.find()
        if resp != None:
            return resp
        else:
            self.abort(404, "Version document does not exist")

    def post(self):
        permchecker = userauth.default(self)
        payload = self.request.json_body
        mongo_schema_uri = validators.schema_uri('mongo', 'filetype.json')
        mongo_validator = validators.decorator_from_schema_path(mongo_schema_uri)
        mongo_validator(permchecker(noop))('PUT', payload=payload)
        result = config.db.filetypes.replace_one({'_id': payload['_id']}, payload, upsert=True)
        if result.acknowledged:
            _id = result.upserted_id if result.upserted_id else payload['_id']
            return {'_id': _id}
        else:
            self.abort(404, 'File type {} not updated'.format(payload['_id']))

    def delete(self, _id):
        permchecker = userauth.default(self)
        permchecker(noop)('DELETE', _id)
        result = config.db.filetypes.delete_one({'_id': _id})
        if result.acknowledged:
            return {'deleted': result.deleted_count}
        else:
            self.abort(404, 'File type {} not removed'.format(_id))