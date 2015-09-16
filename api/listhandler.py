# @author:  Renzo Frigato

import logging
import base
import json
import util
import copy
import os

from . import permchecker
from . import files

log = logging.getLogger('scitran.api')


class ListHandler(base.RequestHandler):

    def __init__(self, request=None, response=None):
        super(ListHandler, self).__init__(request, response)
        self._initialized = None

    def get(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)
        list_name = storage.list_name

        result = perm_checker(storage.apply_change)('GET', _id, elem_match=kwargs)

        if result is None or result.get(list_name) is None or len(result[list_name]) == 0:
            self.abort(404, 'Element not found in list {} of collection {} {}'.format(list_name, storage.coll_name, _id))
        return result[list_name][0]

    def post(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)

        payload = self.request.POST.mixed()
        payload.update(kwargs)
        result = perm_checker(storage.apply_change)('POST', _id, payload=payload)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def put(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)

        result = perm_checker(storage.apply_change)('PUT', _id, elem_match = kwargs, payload = self.request.POST.mixed())

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not updated in list {} of collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def delete(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)
        result = perm_checker(storage.apply_change)('DELETE', _id, elem_match = kwargs)

        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not removed from list {} in collection {} {}'.format(storage.list_name, storage.coll_name, _id))

    def _initialize_request(self, kwargs):
        if self._initialized:
            return self._initialized
        perm_checker = kwargs.pop('permchecker')
        storage = kwargs.pop('storage')
        _id = kwargs.pop('cid', None) or kwargs.pop('_id')

        storage.load_collection(self.app.db)
        container = storage.get_container(_id)
        if container is not None:
            if self.superuser_request:
                perm_checker = permchecker.always_ok
            elif self.public_request:
                perm_checker = permchecker.public_request(self, container)
            else:
                perm_checker = perm_checker(self, container)
        else:
            self.abort(404, 'Element {} not found in collection {}'.format(_id, storage.coll_name))
        self._initialized = (_id, perm_checker, storage)
        return self._initialized

class FileListHandler(ListHandler):

    def __init__(self, request=None, response=None):
        super(FileListHandler, self).__init__(request, response)

    def _check_ticket(self, ticket_id, _id, filename):
        ticket = self.app.db.downloads.find_one({'_id': ticket_id})
        if not ticket:
            self.abort(404, 'no such ticket')
        if ticket['target'] != _id or ticket['filename'] != filename or ticket['ip'] != self.request.client_addr:
            self.abort(400, 'ticket not for this resource or source IP')
        return ticket

    def get(self, *args, **kwargs):
        _id, perm_checker, storage = self._initialize_request(kwargs)
        list_name = storage.list_name
        filename = kwargs.get('filename')
        ticket_id = self.request.GET.get('ticket')
        if ticket_id:
            ticket = self._check_ticket(ticket_id, _id, filename)
            fileinfo = storage.apply_change('GET', _id, elem_match=kwargs)['files'][0]
        else:
            fileinfo = perm_checker(storage.apply_change)('GET', _id, elem_match=kwargs)['files'][0]
        log.error(fileinfo)
        if not fileinfo:
            self.abort(404, 'no such file')
        hash_ = self.request.GET.get('hash')
        if hash_ and hash_ != fileinfo['hash']:
            self.abort(409, 'file exists, hash mismatch')
        filepath = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id), filename)
        if self.request.GET.get('ticket') == '':    # request for download ticket
            ticket = util.download_ticket(self.request.client_addr, 'file', _id, filename, fileinfo['filesize'])
            return {'ticket': self.app.db.downloads.insert_one(ticket).inserted_id}
        else:                                       # authenticated or ticketed (unauthenticated) download
            zip_member = self.request.GET.get('member')
            if self.request.GET.get('info', '').lower() in ('1', 'true'):
                try:
                    with zipfile.ZipFile(filepath) as zf:
                        return [(zi.filename, zi.file_size, datetime.datetime(*zi.date_time)) for zi in zf.infolist()]
                except zipfile.BadZipfile:
                    self.abort(400, 'not a zip file')
            elif self.request.GET.get('comment', '').lower() in ('1', 'true'):
                try:
                    with zipfile.ZipFile(filepath) as zf:
                        self.response.write(zf.comment)
                except zipfile.BadZipfile:
                    self.abort(400, 'not a zip file')
            elif zip_member:
                try:
                    with zipfile.ZipFile(filepath) as zf:
                        self.response.headers['Content-Type'] = util.guess_mimetype(zip_member)
                        self.response.write(zf.open(zip_member).read())
                except zipfile.BadZipfile:
                    self.abort(400, 'not a zip file')
                except KeyError:
                    self.abort(400, 'zip file contains no such member')
            else:
                self.response.app_iter = open(filepath, 'rb')
                self.response.headers['Content-Length'] = str(fileinfo['filesize']) # must be set after setting app_iter
                if self.request.GET.get('view', '').lower() in ('1', 'true'):
                    self.response.headers['Content-Type'] = str(fileinfo.get('mimetype', 'application/octet-stream'))
                else:
                    self.response.headers['Content-Type'] = 'application/octet-stream'
                    self.response.headers['Content-Disposition'] = 'attachment; filename="' + filename + '"'

    def delete(self, *args, **kwargs):
        filename = kwargs.get('filename')
        _id = kwargs.get('cid', None) or kwargs.get('_id')
        result = super(FileListHandler, self).delete(*args, **kwargs)
        filepath = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id), filename)
        if os.path.exists(filepath):
            os.remove(filepath)
            log.info('removed file ' + filepath)
            result['removed'] = 1
        else:
            log.warning(filepath + ' does not exist')
            result['removed'] = 0
        return result

    def put(self, *args, **kwargs):
        fileinfo = super(FileListHandler, self).get(*args, **kwargs)
        # TODO: implement file metadata updates
        self.abort(400, 'PUT is not yet implemented')

    def post(self, *args, **kwargs):
        force = self.request.GET.get('force', '').lower() in ('1', 'true')
        _id, perm_checker, storage = self._initialize_request(kwargs)
        payload = self.request.POST.mixed()
        payload.update(kwargs)
        filename = payload.get('filename')
        file_request = files.FileRequest.from_handler(self, filename)
        file_request.save_temp_file(self.app.config['upload_path'])
        payload.update({
            'filesize': file_request.filesize,
            'filehash': file_request.sha1,
            'filetype': file_request.filetype,
            'flavor': file_request.flavor,
            'mimetype': file_request.mimetype,
            'tags': file_request.tags,
            'metadata': file_request.metadata,
        })
        dest_path = os.path.join(self.app.config['data_path'], str(_id)[-3:] + '/' + str(_id))
        if not force:
            result = perm_checker(storage.apply_change)('POST', _id, payload=payload)
        else:
            container = storage.get_container(_id)
            filepath = os.path.join(tempdir_path, filename)
            for f in container['files']:
                if f['filename'] == filename:
                    if file_request.identical(os.path.join(data_path, filename), f['filehash']):
                        log.debug('Dropping    %s (identical)' % filename)
                        os.remove(filepath)
                        self.abort(409, 'file exists; use force to overwrite')
                    else:
                        log.debug('Replacing   %s' % filename)
                        result = perm_checker(storage.apply_change)('PUT', _id, payload=payload)
                    break
            else:
                result = perm_checker(storage.apply_change)('POST', _id, payload=payload)
        if result.modified_count != 1:
            storage.apply_change('DELETE', _id, payload=payload)
            self.abort(404, 'Element not added in list {} of collection {} {}'.format(storage.list_name, storage.coll_name, _id))
        try:
            file_request.move_temp_file(dest_path)
        except IOError as e:
            result = storage.apply_change('DELETE', _id, payload=payload)
            raise e
        return {'modified': result.modified_count}
