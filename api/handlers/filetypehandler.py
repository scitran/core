from ..web import base
from .. import config
from .. import validators
from ..auth import userauth
from ..dao import noop

class FileType(base.RequestHandler):

    def get(self):
        """Get file types"""
        return config.db.filetypes.find()

    def post(self):
        """
        Insert or replace a file type. Required fields: '_id' and 'regex' where the '_id' is the unique name of
        the file type and 'regex' is a regular expression which is used to figure out the file type from the file name.
        """
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
        """Delete a file type"""
        permchecker = userauth.default(self)
        permchecker(noop)('DELETE', _id)
        result = config.db.filetypes.delete_one({'_id': _id})
        if result.deleted_count:
            return {'deleted': result.deleted_count}
        else:
            self.abort(404, 'File type {} not removed'.format(_id))