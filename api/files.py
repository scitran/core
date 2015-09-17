# @author:  Renzo Frigato

import datetime
import tempfile
import hashlib
import logging
import shutil
import json
import os

from . import util

log = logging.getLogger('scitran.api')


class FileRequest(object):
    """This class provides and interface for file uploads.
    To perform an upload the client of the class should follow these steps:

    1) initialize the request
    2) save a temporary file
    3) check identical
    4) move the temporary file to its destination

    The operations could be safely interleaved with other actions like permission checks or database updates.
    """

    def __init__(self, client_addr, filename, body, received_md5, metadata, tags, flavor):
        self.client_addr = client_addr
        self.filename = filename
        self.body = body
        self.received_md5 = received_md5
        self.metadata = metadata
        self.tags = tags
        self.mimetype = util.guess_mimetype(filename)
        self.filetype = util.guess_filetype(filename, self.mimetype)
        self.flavor = flavor

    def save_temp_file(self, upload_path):
        self.tempdir_path = tempfile.mkdtemp(prefix='.tmp', dir=upload_path)
        success, duration = self._save_temp_file(self.tempdir_path)
        if not success:
            self.abort(400, 'Content-MD5 mismatch.')
        throughput = self.filesize / duration.total_seconds()
        log.info('Received    %s [%s, %s/s] from %s' % (
            self.filename,
            util.hrsize(self.filesize), util.hrsize(throughput),
            self.client_addr))

    def move_temp_file(self, container_path):
        target_filepath = container_path + '/' + self.filename
        temp_filepath = self.tempdir_path + '/' + self.filename
        if not os.path.exists(container_path):
            os.makedirs(container_path)
        shutil.move(temp_filepath, target_filepath)
        os.rmdir(self.tempdir_path)

    def _save_temp_file(self, folder):
        filepath = os.path.join(folder, self.filename)
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        filesize = 0
        start_time = datetime.datetime.utcnow()
        with open(filepath, 'wb') as fd:
            for chunk in iter(lambda: self.body.read(2**20), ''):
                md5.update(chunk)
                sha1.update(chunk)
                filesize += len(chunk)
                fd.write(chunk)
        self.filesize = filesize
        self.md5 = md5.hexdigest()
        self.sha1 = sha1.hexdigest()
        duration = datetime.datetime.utcnow() - start_time
        success = (self.md5 == self.received_md5) if self.received_md5 is not None else True
        return success, duration

    def check_identical(self, filepath, md5):
        filepath1 = os.path.join(self.tempdir_path, filename)
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
            return md5 == self.received_md5

    @classmethod
    def from_handler(cls, handler, filename=None):
        """
        Convenient method to initialize an upload request from the FileListHandler receiving it.
        """
        tags = []
        metadata = {}
        if handler.request.content_type == 'multipart/form-data':
            body = None
            # use cgi lib to parse multipart data without loading all into memory; use tempfile instead
            # FIXME avoid using tempfile; processs incoming stream on the fly
            fs_environ = handler.request.environ.copy()
            fs_environ.setdefault('CONTENT_LENGTH', '0')
            fs_environ['QUERY_STRING'] = ''
            form = cgi.FieldStorage(fp=handler.request.body_file, environ=fs_environ, keep_blank_values=True)
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

            if 'Content-MD5' not in handler.request.headers:
                handler.abort(400, 'Request must contain a valid "Content-MD5" header.')
            try:
                tags = json.loads(handler.request.GET.get('tags', '[]'))
            except ValueError:
                handler.abort(400, 'invalid "tags" parameter')
            try:
                metadata = json.loads(handler.request.GET.get('metadata', '{}'))
            except ValueError:
                handler.abort(400, 'invalid "metadata" parameter')
            body = handler.request.body_file
        flavor = handler.request.GET.get('flavor', 'data') # TODO: flavor should go away
        if flavor not in ['data', 'attachment']:
            handler.abort(400, 'Query must contain flavor parameter: "data" or "attachment".')
        md5 = handler.request.headers.get('Content-MD5')
        return cls(handler.request.client_addr, filename, body, md5, metadata, tags, flavor)
