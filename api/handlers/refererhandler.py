"""
Module defining RefererHandler and it's subclasses. RefererHandler
generalizes the handling of documents that are not part of the container
hierarchy, are always associated with (referencing) a parent container,
and are stored in their own collection instead of an embedded list on the
container (eg. ListHandler)
"""

from .. import config
from .. import upload
from ..auth import refererauth, always_ok
from ..dao import containerstorage, noop
from ..web import base
from ..web.request import log_access, AccessType


log = config.log


class RefererHandler(base.RequestHandler):
    def __init__()

    referer_handler_configurations = {
        'analyses': {
            'storage': 
            'storage_schema_file': 'analysis.json',
            'payload_schema_file': 'analysis.json',
            'permchecker': refererauth.default_referer,
        },
    }


    def _get_permchecker(self, container):
        if self.superuser_request:
            return always_ok
        elif self.public_request:
            return refererauth.public_request(self, container)
        else:
            permchecker = self.config['permchecker']
            return permchecker(self, container)


class AnalysesHandler(RefererHandler):
    def post(self, cont_name, cid, **kwargs):
        """
        Default behavior:
            Creates an analysis object and uploads supplied input
            and output files.
        When param ``job`` is true:
            Creates an analysis object and job object that reference
            each other via ``job`` and ``destination`` fields. Job based
            analyses are only allowed at the session level.
        """
        parent = self.storage.get_parent(cont_name, cid)
        permchecker = self._get_permchecker(container=container)
        permchecker(noop)('POST', container)

        if self.is_true('job'):
            if cont_name == 'sessions':
                payload = self.request.json_body
                payload_validator(payload.get('analysis',{}), 'POST')
                analysis = payload.get('analysis')
                job = payload.get('job')
                if job is None or analysis is None:
                    self.abort(400, 'JSON body must contain map for "analysis" and "job"')
                result = self.storage.create_job_and_analysis(cont_name, _id, analysis, job, self.origin)
                return {'_id': result['analysis']['_id']}
            else:
                self.abort(400, 'Analysis created via a job must be at the session level')

        # _id = kwargs.pop('cid')
        # permchecker, storage, _, payload_validator, _ = self._initialize_request(cont_name, list_name, _id)
        # permchecker(noop)('POST', _id=_id)

        payload = upload.process_upload(self.request, upload.Strategy.analysis, origin=self.origin)
        analysis = self.storage.default_analysis(self.origin)
        analysis.update(payload)
        result = self.storage.exec_op('POST', _id=_id, payload=analysis)

        if result.modified_count == 1:
            return {'_id': analysis['_id']}
        else:
            self.abort(500, 'Element not added in list analyses of container {} {}'.format(cont_name, _id))


    def _get_parent_container(self, payload):
        if not self.config.get('parent_storage'):
            return None, None
        parent_storage = self.config['parent_storage']
        parent_id_property = parent_storage.cont_name[:-1]
        parent_id = payload.get(parent_id_property)
        if parent_id:
            parent_storage.dbc = config.db[parent_storage.cont_name]
            parent_container = parent_storage.get_container(parent_id)
            if parent_container is None:
                self.abort(404, 'Element {} not found in container {}'.format(parent_id, parent_storage.cont_name))
        else:
            parent_container = None
        return parent_container, parent_id_property


    @log_access(AccessType.delete_analysis)
    def delete(self, **kwargs):
        # Overriding base class delete to audit action before completion
        _id = kwargs.pop('cid')
        permchecker, storage, _, _, keycheck = self._initialize_request(cont_name, list_name, _id, query_params=kwargs)

        permchecker(noop)('DELETE', _id=_id, query_params=kwargs)
        self.log_user_access(AccessType.delete_file, cont_name=cont_name, cont_id=_id)
        try:
            result = keycheck(storage.exec_op)('DELETE', _id, query_params=kwargs)
        except APIStorageException as e:
            self.abort(400, e.message)
        if result.modified_count == 1:
            return {'modified': result.modified_count}
        else:
            self.abort(404, 'Element not removed from list {} in container {} {}'.format(storage.list_name, storage.cont_name, _id))


    def add_note(self, cont_name, cid, child_name, _id):
        _id = kwargs.pop('cid')
        analysis_id = kwargs.get('_id')
        permchecker, storage, _, _, _ = self._initialize_request(cont_name, list_name, _id)
        payload = self.request.json_body

        notes_schema_file = list_handler_configurations[cont_name]['notes']['storage_schema_file']
        input_schema_uri = validators.schema_uri('input', notes_schema_file)
        input_validator = validators.from_schema_path(input_schema_uri)
        input_validator(payload, 'POST')

        payload['_id'] = str(bson.objectid.ObjectId())
        payload['user'] = payload.get('user', self.uid)
        payload['created'] = datetime.datetime.utcnow()
        permchecker(noop)('POST', _id=_id)
        result = storage.add_note(_id=_id, analysis_id=analysis_id, payload=payload)
        if result.modified_count == 1:
            return {'modified':result.modified_count}
        else:
            self.abort(404, 'Element not added in list {} of container {} {}'.format(storage.list_name, storage.cont_name, _id))


    def delete_note(self, cont_name, list_name, **kwargs):
        _id = kwargs.pop('cid')
        analysis_id = kwargs.pop('_id')
        permchecker, storage, _, _, _ = self._initialize_request(cont_name, list_name, _id)
        note_id = kwargs.get('note_id')
        permchecker(noop)('DELETE', _id=_id)
        result = storage.delete_note(_id=_id, analysis_id=analysis_id, note_id=note_id)
        if result.modified_count == 1:
            return {'modified': result.modified_count}
        else:
            self.abort(404, 'Note not removed from analysis {}'.format(analysis_id))


    def download(self, **kwargs):
        """
        .. http:get:: /api/(cont_name)/(cid)/analyses/(analysis_id)/files/(file_name)

            Download a file from an analysis or download a tar of all files

            When no filename is provided, a tar of all input and output files is created.
            The first request to this endpoint without a ticket ID generates a download ticket.
            A request to this endpoint with a ticket ID downloads the file(s).
            If the analysis object is tied to a job, the input file(s) are inlfated from
            the job's ``input`` array.

            :param cont_name: one of ``projects``, ``sessions``, ``collections``
            :type cont_name: string

            :param cid: Container ID
            :type cid: string

            :param analysis_id: Analysis ID
            :type analysis_id: string

            :param filename: (Optional) Filename of specific file to download
            :type cid: string

            :query string ticket: Download ticket ID

            :statuscode 200: no error
            :statuscode 404: No files on analysis ``analysis_id``
            :statuscode 404: Could not find file ``filename`` on analysis ``analysis_id``

            **Example request without ticket ID**:

            .. sourcecode:: http

                GET /api/sessions/57081d06b386a6dc79ca383c/analyses/5751cd3781460100a66405c8/files HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/json; charset=utf-8
                {
                  "ticket": "57f2af23-a94c-426d-8521-11b2e8782020",
                  "filename": "analysis_5751cd3781460100a66405c8.tar",
                  "file_cnt": 3,
                  "size": 4525137
                }

            **Example request with ticket ID**:

            .. sourcecode:: http

                GET /api/sessions/57081d06b386a6dc79ca383c/analyses/5751cd3781460100a66405c8/files?ticket=57f2af23-a94c-426d-8521-11b2e8782020 HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/octet-stream
                Content-Disposition: attachment; filename=analysis_5751cd3781460100a66405c8.tar;

            **Example Request with filename**:

            .. sourcecode:: http

                GET /api/sessions/57081d06b386a6dc79ca383c/analyses/5751cd3781460100a66405c8/files/exampledicom.zip?ticket= HTTP/1.1
                Host: demo.flywheel.io
                Accept: */*


            **Response**:

            .. sourcecode:: http

                HTTP/1.1 200 OK
                Vary: Accept-Encoding
                Content-Type: application/json; charset=utf-8
                {
                  "ticket": "57f2af23-a94c-426d-8521-11b2e8782020",
                  "filename": "exampledicom.zip",
                  "file_cnt": 1,
                  "size": 4525137
                }


        """
        _id = kwargs.pop('cid')
        permchecker, storage, _, _, _ = self._initialize_request(cont_name, list_name, _id)
        filename = kwargs.get('name')
        ticket_id = self.get_param('ticket')
        ticket = None
        if ticket_id is None:
            permchecker(noop)('GET', _id=_id)
        elif ticket_id != '':
            ticket = self._check_ticket(ticket_id, _id, filename)
            if not self.origin.get('id'):
                self.origin = ticket.get('origin')
        analysis_id = kwargs.get('_id')
        fileinfo = storage.get_fileinfo(_id, analysis_id, filename)
        if fileinfo is None:
            error_msg = 'No files on analysis {}'.format(analysis_id)
            if filename:
                error_msg = 'Could not find file {} on analysis {}'.format(filename, analysis_id)
            self.abort(404, error_msg)
        if ticket_id == '':
            if filename:
                total_size = fileinfo[0]['size']
                file_cnt = 1
                ticket = util.download_ticket(self.request.client_addr, 'file', _id, filename, total_size, origin=self.origin)
            else:
                targets, total_size, file_cnt = self._prepare_batch(fileinfo)
                label = util.sanitize_string_to_filename(self.storage.get_container(_id).get('label', 'No Label'))
                filename = 'analysis_' + label + '.tar'
                ticket = util.download_ticket(self.request.client_addr, 'batch', targets, filename, total_size, origin=self.origin)
            return {
                'ticket': config.db.downloads.insert_one(ticket).inserted_id,
                'size': total_size,
                'file_cnt': file_cnt,
                'filename': filename
            }
        else:
            if not filename:
                if ticket:
                    self._send_batch(ticket)
                else:
                    self.abort(400, 'batch downloads require a ticket')
            elif not fileinfo:
                self.abort(404, "{} doesn't exist".format(filename))
            else:
                fileinfo = fileinfo[0]
                filepath = os.path.join(
                    config.get_item('persistent', 'data_path'),
                    util.path_from_hash(fileinfo['hash'])
                )
                filename = fileinfo['name']

                # Request for info about zipfile
                if self.is_true('info'):
                    try:
                        info = FileListHandler.build_zip_info(filepath)
                    except zipfile.BadZipfile:
                        self.abort(400, 'not a zip file')
                    return info

                # Request to download zipfile member
                elif self.get_param('member') is not None:
                    zip_member = self.get_param('member')
                    try:
                        with zipfile.ZipFile(filepath) as zf:
                            self.response.headers['Content-Type'] = util.guess_mimetype(zip_member)
                            self.response.write(zf.open(zip_member).read())
                    except zipfile.BadZipfile:
                        self.abort(400, 'not a zip file')
                    except KeyError:
                        self.abort(400, 'zip file contains no such member')
                    # log download if we haven't already for this ticket
                    if ticket:
                        if not ticket.get('logged', False):
                            self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=_id)
                            config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
                    else:
                        self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=_id)

                # Request to download the file itself
                else:
                    self.response.app_iter = open(filepath, 'rb')
                    self.response.headers['Content-Length'] = str(fileinfo['size']) # must be set after setting app_iter
                    if self.is_true('view'):
                        self.response.headers['Content-Type'] = str(fileinfo.get('mimetype', 'application/octet-stream'))
                    else:
                        self.response.headers['Content-Type'] = 'application/octet-stream'
                        self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(filename)

            # log download if we haven't already for this ticket
            if ticket:
                ticket = config.db.downloads.find_one({'_id': ticket_id})
                if not ticket.get('logged', False):
                    self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=_id)
                    config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
            else:
                self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=_id)


    def _check_ticket(self, ticket_id, _id, filename):
        ticket = config.db.downloads.find_one({'_id': ticket_id})
        if not ticket:
            self.abort(404, 'no such ticket')
        if ticket['ip'] != self.request.client_addr:
            self.abort(400, 'ticket not for this source IP')
        if not filename:
            return self._check_ticket_for_batch(ticket)
        if ticket.get('filename') != filename or ticket['target'] != _id:
            self.abort(400, 'ticket not for this resource')
        return ticket


    def _check_ticket_for_batch(self, ticket):
        if ticket.get('type') != 'batch':
            self.abort(400, 'ticket not for this resource')
        return ticket


    def _prepare_batch(self, fileinfo):
        ## duplicated code from download.py
        ## we need a way to avoid this
        targets = []
        total_size = total_cnt = 0
        data_path = config.get_item('persistent', 'data_path')
        for f in fileinfo:
            filepath = os.path.join(data_path, util.path_from_hash(f['hash']))
            if os.path.exists(filepath): # silently skip missing files
                targets.append((filepath, 'analyses/' + f['name'], f['size']))
                total_size += f['size']
                total_cnt += 1
        return targets, total_size, total_cnt


    def _send_batch(self, ticket):
        self.response.app_iter = download.archivestream(ticket)
        self.response.headers['Content-Type'] = 'application/octet-stream'
        self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(ticket['filename'])
