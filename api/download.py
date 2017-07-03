import bson
import json
import pytz
import os.path
import tarfile
import datetime
import cStringIO

from .web import base
from . import config
from . import util
from . import validators
import os
import zipfile
from .dao import containerstorage, noop
from .dao.containerutil import SINGULAR_TO_PLURAL
from .web.request import AccessType
from .handlers.listhandler import FileListHandler
from .auth import always_ok, containerauth

log = config.log


def _filter_check(property_filter, property_values):
    minus = set(property_filter.get('-', []))
    plus = set(property_filter.get('+', []))
    if not minus.isdisjoint(property_values):
        return False
    if plus and plus.isdisjoint(property_values):
        return False
    return True


def _append_targets(targets, container, prefix, total_size, total_cnt, optional, data_path, filters):
    for f in container.get('files', []):
        if filters:
            filtered = True
            for filter_ in filters:
                type_as_list = [f['type']] if f.get('type') else []
                if (
                    _filter_check(filter_.get('tags', {}), f.get('tags', [])) and
                    _filter_check(filter_.get('types', {}), type_as_list)
                    ):
                    filtered = False
                    break
            if filtered:
                continue
        if optional or not f.get('optional', False):
            filepath = os.path.join(data_path, util.path_from_hash(f['hash']))
            if os.path.exists(filepath): # silently skip missing files
                targets.append((filepath, prefix + '/' + f['name'], f['size']))
                total_size += f['size']
                total_cnt += 1
    return total_size, total_cnt

def symlinkarchivestream(ticket, data_path):
    for filepath, arcpath, _ in ticket['target']:
        t = tarfile.TarInfo(name=arcpath)
        t.type = tarfile.SYMTYPE
        t.linkname = os.path.relpath(filepath, data_path)
        yield t.tobuf()
    stream = cStringIO.StringIO()
    with tarfile.open(mode='w|', fileobj=stream) as _:
        pass
    yield stream.getvalue() # get tar stream trailer
    stream.close()

def archivestream(ticket):
    BLOCKSIZE = 512
    CHUNKSIZE = 2**20  # stream files in 1MB chunks
    stream = cStringIO.StringIO()
    with tarfile.open(mode='w|', fileobj=stream) as archive:
        for filepath, arcpath, _ in ticket['target']:
            yield archive.gettarinfo(filepath, arcpath).tobuf()
            with open(filepath, 'rb') as fd:
                chunk = ''
                for chunk in iter(lambda: fd.read(CHUNKSIZE), ''): # pylint: disable=cell-var-from-loop
                    yield chunk
                if len(chunk) % BLOCKSIZE != 0:
                    yield (BLOCKSIZE - (len(chunk) % BLOCKSIZE)) * b'\0'
    yield stream.getvalue() # get tar stream trailer
    stream.close()

class Download(base.RequestHandler):

    def _bulk_preflight_archivestream(self, file_refs):
        data_path = config.get_item('persistent', 'data_path')
        arc_prefix =  self.get_param('prefix', 'scitran')
        file_cnt = 0
        total_size = 0
        targets = []

        for fref in file_refs:
            cont_name   = SINGULAR_TO_PLURAL.get(fref.get('container_name',''))
            cont_id     = fref.get('container_id', '')
            filename    = fref.get('filename', '')

            if cont_name not in ['projects', 'sessions', 'acquisitions', 'analyses']:
                self.abort(400, 'Bulk download only supports files in projects, sessions, analyses and acquisitions')
            file_obj = None
            try:
                # Try to find the file reference in the database (filtering on user permissions)
                bid = bson.ObjectId(cont_id)
                query = {'_id': bid}
                if not self.superuser_request:
                    query['permissions._id'] = self.uid
                file_obj = config.db[cont_name].find_one(
                    query,
                    {'files': { '$elemMatch': {
                        'name': filename
                    }}
                })['files'][0]
            except Exception: # pylint: disable=broad-except
                # self.abort(404, 'File {} on Container {} {} not found'.format(filename, cont_name, cont_id))
                # silently skip missing files/files user does not have access to
                continue

            filepath = os.path.join(data_path, util.path_from_hash(file_obj['hash']))
            if os.path.exists(filepath): # silently skip missing files
                targets.append((filepath, cont_name+'/'+cont_id+'/'+file_obj['name'], file_obj['size']))
                total_size += file_obj['size']
                file_cnt += 1

        if len(targets) > 0:
            filename = arc_prefix + '_ '+datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S') + '.tar'
            ticket = util.download_ticket(self.request.client_addr, 'batch', targets, filename, total_size)
            config.db.downloads.insert_one(ticket)
            return {'ticket': ticket['_id'], 'file_cnt': file_cnt, 'size': total_size}
        else:
            self.abort(404, 'No files requested could be found')


    def _preflight_archivestream(self, req_spec, collection=None):
        data_path = config.get_item('persistent', 'data_path')
        arc_prefix = self.get_param('prefix', 'scitran')
        file_cnt = 0
        total_size = 0
        targets = []

        used_subpaths = {}
        base_query = {}
        if not self.superuser_request:
            base_query['permissions._id'] = self.uid

        for item in req_spec['nodes']:

            item_id = bson.ObjectId(item['_id'])
            base_query['_id'] = item_id

            if item['level'] == 'project':
                project = config.db.projects.find_one(base_query, ['group', 'label', 'files'])
                if not project:
                    # silently skip missing objects/objects user does not have access to
                    continue

                prefix = '/'.join([arc_prefix, project['group'], project['label']])
                total_size, file_cnt = _append_targets(targets, project, prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

                sessions = config.db.sessions.find({'project': item_id}, ['label', 'files', 'uid', 'timestamp', 'timezone', 'subject'])
                session_dict = {session['_id']: session for session in sessions}
                acquisitions = config.db.acquisitions.find({'session': {'$in': session_dict.keys()}}, ['label', 'files', 'session', 'uid', 'timestamp', 'timezone'])
                session_prefixes = {}

                subject_dict = {}
                subject_prefixes = {}
                for session in session_dict.itervalues():
                    if session.get('subject'):
                        code = session['subject'].get('code', 'unknown_subject')
                        # This is bad and we should try to combine these somehow,
                        # or at least make sure we get all the files
                        subject_dict[code] = session['subject']

                for code, subject in subject_dict.iteritems():
                    subject_prefix = prefix + '/' + self._path_from_container(subject, used_subpaths, project['_id'])
                    subject_prefixes[code] = subject_prefix
                    total_size, file_cnt = _append_targets(targets, subject, subject_prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

                for session in session_dict.itervalues():
                    subject_code = session['subject'].get('code', 'unknown_subject')
                    subject = subject_dict[subject_code]
                    session_prefix = subject_prefixes[subject_code] + '/' + self._path_from_container(session, used_subpaths, subject_code)
                    session_prefixes[session['_id']] = session_prefix
                    total_size, file_cnt = _append_targets(targets, session, session_prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

                for acq in acquisitions:
                    session = session_dict[acq['session']]
                    acq_prefix = session_prefixes[session['_id']] + '/' + self._path_from_container(acq, used_subpaths, session['_id'])
                    total_size, file_cnt = _append_targets(targets, acq, acq_prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))


            elif item['level'] == 'session':
                session = config.db.sessions.find_one(base_query, ['project', 'label', 'files', 'uid', 'timestamp', 'timezone', 'subject'])
                if not session:
                    # silently skip missing objects/objects user does not have access to
                    continue

                project = config.db.projects.find_one({'_id': session['project']}, ['group', 'label'])
                subject = session.get('subject', {'code': 'unknown_subject'})
                if not subject.get('code'):
                    subject['code'] = 'unknown_subject'
                prefix = project['group'] + '/' + project['label'] + '/' + self._path_from_container(subject, used_subpaths, project['_id']) + '/' + self._path_from_container(session, used_subpaths, project['_id'])
                total_size, file_cnt = _append_targets(targets, session, prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

                # If the param `collection` holding a collection id is not None, filter out acquisitions that are not in the collection
                a_query = {'session': item_id}
                if collection:
                    a_query['collections'] = bson.ObjectId(collection)
                acquisitions = config.db.acquisitions.find(a_query, ['label', 'files', 'uid', 'timestamp', 'timezone'])

                for acq in acquisitions:
                    acq_prefix = prefix + '/' + self._path_from_container(acq, used_subpaths, session['_id'])
                    total_size, file_cnt = _append_targets(targets, acq, acq_prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

            elif item['level'] == 'acquisition':
                acq = config.db.acquisitions.find_one(base_query, ['session', 'label', 'files', 'uid', 'timestamp', 'timezone'])
                if not acq:
                    # silently skip missing objects/objects user does not have access to
                    continue

                session = config.db.sessions.find_one({'_id': acq['session']}, ['project', 'label', 'uid', 'timestamp', 'timezone', 'subject'])
                subject = session.get('subject', {'code': 'unknown_subject'})
                if not subject.get('code'):
                    subject['code'] = 'unknown_subject'

                project = config.db.projects.find_one({'_id': session['project']}, ['group', 'label'])
                prefix = project['group'] + '/' + project['label'] + '/' + self._path_from_container(subject, used_subpaths, project['_id']) + '/' + self._path_from_container(session, used_subpaths, project['_id']) + '/' + self._path_from_container(acq, used_subpaths, session['_id'])
                total_size, file_cnt = _append_targets(targets, acq, prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

            elif item['level'] == 'analysis':
                analysis = config.db.analyses.find_one(base_query, ['parent', 'label', 'files', 'uid', 'timestamp'])
                if not analysis:
                    # silently skip missing objects/objects user does not have access to
                    continue

                session = config.db.sessions.find_one({'_id': analysis['parent']['id']}, ['project', 'label', 'uid', 'timestamp', 'timezone', 'subject'])
                subject = session.get('subject', {'code': 'unknown_subject'})
                if not subject.get('code'):
                    subject['code'] = 'unknown_subject'

                project = config.db.projects.find_one({'_id': session['project']}, ['group', 'label'])
                prefix = project['group'] + '/' + project['label'] + '/' + self._path_from_container(subject, used_subpaths, project['_id']) + '/' + self._path_from_container(session, used_subpaths, project['_id']) + '/' + self._path_from_container(analysis, used_subpaths, session['_id'])
                total_size, file_cnt = _append_targets(targets, analysis, prefix, total_size, file_cnt, req_spec['optional'], data_path, req_spec.get('filters'))

        if len(targets) > 0:
            log.debug(json.dumps(targets, sort_keys=True, indent=4, separators=(',', ': ')))
            filename = arc_prefix + '_' + datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S') + '.tar'
            ticket = util.download_ticket(self.request.client_addr, 'batch', targets, filename, total_size)
            config.db.downloads.insert_one(ticket)
            return {'ticket': ticket['_id'], 'file_cnt': file_cnt, 'size': total_size}
        else:
            self.abort(404, 'No requested containers could be found')

    def _path_from_container(self, container, used_subpaths, parent_id):
        def _find_new_path(path, list_used_subpaths):
            """from the input path finds a path that hasn't been used"""
            path = str(path).replace('/', '_')
            if path not in list_used_subpaths:
                return path
            i = 0
            while True:
                modified_path = path + '_' + str(i)
                if modified_path not in list_used_subpaths:
                    return modified_path
                i += 1
        path = None
        if not path and container.get('label'):
            path = container['label']
        if not path and container.get('timestamp'):
            timezone = container.get('timezone')
            if timezone:
                path = pytz.timezone('UTC').localize(container['timestamp']).astimezone(pytz.timezone(timezone)).strftime('%Y%m%d_%H%M')
            else:
                path = container['timestamp'].strftime('%Y%m%d_%H%M')
        if not path and container.get('uid'):
            path = container['uid']
        if not path and container.get('code'):
            path = container['code']
        if not path:
            path = 'untitled'
        path = _find_new_path(path, used_subpaths.get(parent_id, []))
        used_subpaths[parent_id] = used_subpaths.get(parent_id, []) + [path]
        return path

    def download(self):
        """Download files or create a download ticket"""
        ticket_id = self.get_param('ticket')
        if ticket_id:
            ticket = config.db.downloads.find_one({'_id': ticket_id})
            if not ticket:
                self.abort(404, 'no such ticket')
            if ticket['ip'] != self.request.client_addr:
                self.abort(400, 'ticket not for this source IP')
            if self.get_param('symlinks'):
                self.response.app_iter = symlinkarchivestream(ticket, config.get_item('persistent', 'data_path'))
            else:
                self.response.app_iter = archivestream(ticket)
            self.response.headers['Content-Type'] = 'application/octet-stream'
            self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(ticket['filename'])
            for project_id in ticket['projects']:
                config.db.projects.update_one({'_id': project_id}, {'$inc': {'counter': 1}})
        else:
            req_spec = self.request.json_body

            if self.is_true('bulk'):
                return self._bulk_preflight_archivestream(req_spec.get('files', []))
            else:
                payload_schema_uri = validators.schema_uri('input', 'download.json')
                validator = validators.from_schema_path(payload_schema_uri)
                validator(req_spec, 'POST')
                log.debug(json.dumps(req_spec, sort_keys=True, indent=4, separators=(',', ': ')))

                return self._preflight_archivestream(req_spec, collection=self.get_param('collection'))

    def analysis_download(self, cont_name, cid, _id, filename=None):
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
        storage = containerstorage.AnalysisStorage()
        permchecker = containerauth.default_container(storage)
        ticket_id = self.get_param('ticket')
        ticket = None
        if ticket_id is None:
            permchecker(noop)('GET')
        elif ticket_id != '':
            ticket = self._check_ticket(ticket_id, cid, filename)
            if not self.origin.get('id'):
                self.origin = ticket.get('origin')

        analysis = storage.get_container(_id)
        fileinfo = analysis.get('files', [])
        if filename:
            fileinfo = [fi for fi in fileinfo if fi['name'] == filename]

        if not fileinfo:
            error_msg = 'No files on analysis {}'.format(_id)
            if filename:
                error_msg = 'Could not find file {} on analysis {}'.format(filename, _id)
            self.abort(404, error_msg)
        if ticket_id == '':
            if filename:
                total_size = fileinfo[0]['size']
                file_cnt = 1
                ticket = util.download_ticket(self.request.client_addr, 'file', cid, filename, total_size, origin=self.origin)
            else:
                targets, total_size, file_cnt = self._prepare_batch(fileinfo)
                label = util.sanitize_string_to_filename(storage.get_container(_id).get('label', 'No Label'))
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
                            self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid)
                            config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
                    else:
                        self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid)

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
                    self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid)
                    config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
            else:
                self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid)


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

    def input_validator(self):
        input_schema_uri = validators.schema_uri('input', self.payload_schema_file)
        input_validator = validators.from_schema_path(input_schema_uri)
        return input_validator

    def get_permchecker(self, container, permchecker):
        if self.superuser_request:
            return always_ok
        elif self.public_request:
            return containerauth.public_request(self, container=container)
        else:
            # NOTE The handler (self) is passed implicitly
            return permchecker(container=container)

    def _send_batch(self, ticket):
        self.response.app_iter = archivestream(ticket)
        self.response.headers['Content-Type'] = 'application/octet-stream'
        self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(ticket['filename'])
