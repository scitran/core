import datetime
import hashlib
import logging
import shutil
import json
import cgi
import os

from . import util

log = logging.getLogger('scitran.api')

class HashingFile(file):
    def __init__(self, file_path, hash_alg):
        super(HashingFile, self).__init__(file_path, "w+b")
        self.hash_alg = hashlib.new(hash_alg)

    def write(self, data):
        self.hash_alg.update(data)
        return file.write(self, data)

    def get_hash(self):
        return self.hash_alg.hexdigest()

class HashingFieldStorage(cgi.FieldStorage):

    bufsize = 2**20

    def __init__(self, *args, **kwargs):
        self.hash_alg = kwargs.pop('hash_alg')
        self.upload_dir = kwargs.pop('upload_dir')
        kwargs['environ'] = kwargs['environ'].copy()
        kwargs['environ'].setdefault('CONTENT_LENGTH', '0')
        kwargs['environ']['QUERY_STRING'] = ''
        super(HashingFieldStorage, self).__init__(*args, **kwargs)

    def make_file(self, binary=None):
        return HashingFile(os.path.join(self.upload_dir, self.filename), self.hash_alg)


class FileStore(object):
    """This class provides and interface for file uploads.
    To perform an upload the client of the class should follow these steps:

    1) initialize the request
    2) save a temporary file
    3) check identical
    4) move the temporary file to its destination

    The operations could be safely interleaved with other actions like permission checks or database updates.
    """

    def __init__(self, request, dest_path, filename=None, hash_alg='sha384'):
        self.client_addr = request.client_addr
        self.body = request.body_file
        self.environ = request.environ
        self.hash_alg = hash_alg
        self.path = dest_path
        if request.content_type == 'multipart/form-data':
            self._save_multipart_file(dest_path)
        else:
            self._save_body_file(dest_path, filename)
        self.filename = self.received_file.filename
        self.mimetype = util.guess_mimetype(self.filename)
        self.filetype = util.guess_filetype(self.filename, self.mimetype)
        self.hash = self.received_file.get_hash()
        self.size = os.path.getsize(os.path.join(self.app.path, self.filename))

    def _save_multipart_file(self, dest_path):
        form = HashingFieldStorage(upload_dir=dest_path, fp=self.body_file, environ=self.request.environ, keep_blank_values=True, hash_alg='sha384')
        self.received_file = form['file']
        self.tags = form.get('tags')
        self.metadata = form.get('metadata')

    def _save_body_file(self, dest_path, filename):
        if not filename:
            raise FileStoreError('filename is required for body uploads')
        self.received_file = HashingFile(os.path.join(dest_path, filename))
        for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
            self.received_file.write(chunk)
        self.tags = None
        self.metadata = None

    def move_file(self, target_path):
        target_filepath = target_path + '/' + self.filename
        filepath = self.path + '/' + self.filename
        if not os.path.exists(target_path):
            os.makedirs(target_path)
        shutil.move(filepath, target_filepath)
        self.path = target_path

    def identical(self, filepath, sha384):
        filepath1 = os.path.join(self.path, filename)
        if zipfile.is_zipfile(filepath) and zipfile.is_zipfile(filepath1):
            with zipfile.ZipFile(filepath) as zf1, zipfile.ZipFile(filepath1) as zf2:
                zf1_infolist = sorted(zf1.infolist(), key=lambda zi: zi.filename)
                zf2_infolist = sorted(zf2.infolist(), key=lambda zi: zi.filename)
                if zf1.comment != zf2.comment:
                    return False
                if len(zf1_infolist) != len(zf2_infolist):
                    return False
                for zii, zij in zip(zf1_infolist, zf2_infolist):
                    if zii.CRC != zij.CRC:
                        return False
                else:
                    return True
        else:
            return sha384 == self.sha384

    @classmethod
    def from_handler(cls, handler, filename=None, hash_alg='sha384'):
        """
        Convenient method to initialize an upload request from the FileListHandler receiving it.
        """
        tags = []
        metadata = {}
        if handler.request.content_type == 'multipart/form-data':
            body = None
            # use cgi lib to parse multipart data without loading all into memory; use tempfile instead
            # FIXME avoid using tempfile; processs incoming stream on the fly
            form = cgi.FieldStorage(fp=handler.request.body_file, environ=handler.request.environ.copy(), keep_blank_values=True)
            for fieldname in form:
                field = form[fieldname]
                if fieldname == 'file':
                    body = field.file
                    _, filename = os.path.split(field.filename)
                elif fieldname == 'tags':
                    try:
                        tags = json.loads(field.value)
                    except ValueError:
                        handler.abort(400, 'non-JSON value in "tags" parameter')
                elif fieldname == 'metadata':
                    try:
                        metadata = json.loads(field.value)
                    except ValueError:
                        handler.abort(400, 'non-JSON value in "metadata" parameter')
            if body is None:
                handler.abort(400, 'multipart/form-data must contain a "file" field')
        elif filename is None:
            handler.abort(400, 'Request must contain a filename parameter.')
        else:
            _, filename = os.path.split(filename)
            try:
                tags = json.loads(handler.get_param('tags', '[]'))
            except ValueError:
                handler.abort(400, 'invalid "tags" parameter')
            try:
                metadata = json.loads(handler.get_param('metadata', '{}'))
            except ValueError:
                handler.abort(400, 'invalid "metadata" parameter')
            body = handler.request.body_file
        md5 = handler.request.headers.get('Content-MD5')
        return cls(handler.request.client_addr, filename, body, md5, metadata, tags)
