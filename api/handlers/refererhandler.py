"""
Module defining RefererHandler and it's subclasses. RefererHandler
generalizes the handling of documents that are not part of the container
hierarchy, are always associated with (referencing) a parent container,
and are stored in their own collection instead of an embedded list on the
container (eg. ListHandler)
"""

import bson
import os
import zipfile
import datetime
from abc import ABCMeta, abstractproperty

from .. import config
from .. import upload
from .. import util
from .. import validators
from ..auth import containerauth, always_ok
from ..dao import containerstorage, noop
from ..dao.basecontainerstorage import ContainerStorage
from ..dao.containerutil import singularize
from ..web import base
from ..web.errors import APIStorageException, InputValidationException
from ..web.request import log_access, AccessType
from .listhandler import FileListHandler


log = config.log


class RefererHandler(base.RequestHandler):
    __metaclass__ = ABCMeta

    storage = abstractproperty()
    payload_schema_file = abstractproperty()
    permchecker = containerauth.default_referer

    @property
    def input_validator(self):
        input_schema_uri = validators.schema_uri('input', self.payload_schema_file)
        input_validator = validators.from_schema_path(input_schema_uri)
        return input_validator

    @property
    def update_validator(self):
        update_schema_uri = validators.schema_uri('input', self.update_payload_schema_file)
        update_validator = validators.from_schema_path(update_schema_uri)
        return update_validator

    def get_permchecker(self, parent_container):
        if self.superuser_request:
            return always_ok
        elif self.public_request:
            return containerauth.public_request(self, container=parent_container)
        else:
            # NOTE The handler (self) is passed implicitly
            return self.permchecker(parent_container=parent_container)


class AnalysesHandler(RefererHandler):
    storage = containerstorage.AnalysisStorage()
    payload_schema_file = 'analysis.json'
    update_payload_schema_file = 'analysis-update.json'


    def post(self, cont_name, cid):
        """
        Default behavior:
            Creates an analysis object and uploads supplied input
            and output files.
        When param ``job`` is true:
            Creates an analysis object and job object that reference
            each other via ``job`` and ``destination`` fields. Job based
            analyses are only allowed at the session level.
        """
        parent = ContainerStorage.factory(cont_name).get_container(cid)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('POST')

        try:
            analysis = self.request.json_body
            self.input_validator(analysis, 'POST')
        except ValueError:
            # Legacy analysis - accept direct file uploads (inputs and outputs)
            analysis = upload.process_upload(self.request, upload.Strategy.analysis, origin=self.origin)

        uid = None if self.superuser_request else self.uid
        result = self.storage.create_el(analysis, cont_name, cid, self.origin, uid)
        return {'_id': result.inserted_id}

    @validators.verify_payload_exists
    def put(self, **kwargs):
        _id = kwargs.pop('_id')
        parent = self.storage.get_parent(_id)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('PUT')


        payload = self.request.json_body
        self.update_validator(payload, 'PUT')

        payload['modified'] = datetime.datetime.utcnow()

        result = self.storage.update_el(_id, payload)

        if result.modified_count == 1:
            return {'modified': result.modified_count}
        else:
            self.abort(404, 'Element not updated in container {} {}'.format(self.storage.cont_name, _id))

    @validators.verify_payload_exists
    def modify_info(self, **kwargs):
        _id = kwargs.get('_id')

        analysis = self.storage.get_container(_id)
        parent = self.storage.get_parent(_id, cont=analysis)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('PUT')

        payload = self.request.json_body
        validators.validate_data(payload, 'info_update.json', 'input', 'POST')

        self.storage.modify_info(_id, payload)

        return

    def get(self, **kwargs):
        _id = kwargs.get('_id')
        analysis = self.storage.get_container(_id)
        parent = self.storage.get_parent(_id, cont=analysis)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('GET')

        if self.is_true('inflate_job'):
            self.storage.inflate_job_info(analysis)

        self.log_user_access(AccessType.view_container, cont_name=analysis['parent']['type'], cont_id=analysis['parent']['id'])
        return analysis

    def get_all(self, cont_name, cid, **kwargs):
        """
        The possible endpoints for this method are:
        /{cont_name}/{id}/analyses
        /{cont_name}/{id}/{sub_cont_name}/analyses
        /{cont_name}/{id}/all/analyses
        """
        if not cont_name == 'groups':
            cid = bson.ObjectId(cid)
        # Check that user has permission to container
        container = ContainerStorage.factory(cont_name).get_container(cid)
        if not container:
            self.abort(404, 'Element not found in container {} {}'.format(cont_name, cid))
        permchecker = self.get_permchecker(container)
        permchecker(noop)('GET')
        # cont_level is the sub_container name for which the analysis.parent.type should be
        cont_level = kwargs.get('sub_cont_name')
        cont_names = {'groups':0, 'projects':1, 'sessions':2, 'acquisitions':3}
        sub_cont_names = {'projects':0, 'sessions':1, 'acquisitions':2, 'all':3}

        # Check that the url is valid
        if cont_name not in cont_names.keys():
            self.abort(400, "Analysis lists not supported for {}.".format(cont_name))
        elif cont_level:
            if cont_names[cont_name] > sub_cont_names.get(cont_level, -1):
                self.abort(400, "{} not a child of {} or 'all'.".format(cont_level, cont_name))

        if cont_level:
            parent_tree = ContainerStorage.get_top_down_hierarchy(cont_name, cid)
            # We only need a list of all the ids, no need for the tree anymore
            if cont_level == 'all':
                parents = [pid for parent in parent_tree.keys() for pid in parent_tree[parent]]
            else:
                parents = [pid for pid in parent_tree[cont_level]]
            # We set User to None because we check for permission when finding the parents
            analyses = containerstorage.AnalysisStorage().get_all_el({'parent.id':{'$in':parents}},None,{'info': 0, 'files.info': 0})
        else:
            analyses = containerstorage.AnalysisStorage().get_all_el({'parent.id':cid, 'parent.type':singularize(cont_name)},None,{'info': 0, 'files.info': 0})
        return analyses



    @log_access(AccessType.delete_analysis)
    def delete(self, cont_name, cid, _id):
        parent = self.storage.get_parent(_id)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('DELETE')

        try:
            result = self.storage.delete_el(_id)
        except APIStorageException as e:
            self.abort(400, e.message)
        if result.modified_count == 1:
            return {'deleted': result.modified_count}
        else:
            self.abort(404, 'Analysis {} not removed from container {} {}'.format(_id, cont_name, cid))


    def upload(self, **kwargs):
        """Upload ad-hoc analysis outputs generated offline."""
        _id = kwargs.get('_id')
        analysis = self.storage.get_container(_id)
        parent = self.storage.get_parent(_id, cont=analysis)
        permchecker = self.get_permchecker(parent)
        permchecker(noop)('POST')

        if analysis.get('job'):
            raise InputValidationException('Analysis created via a job does not allow file uploads')
        elif analysis.get('files'):
            raise InputValidationException('Analysis already has outputs and does not allow repeated file uploads')

        upload.process_upload(self.request, upload.Strategy.targeted_multi, container_type='analysis', id_=_id, origin=self.origin)


    def download(self, **kwargs):
        """
        .. http:get:: /api/(cont_name)*/(cid)*/analyses/(analysis_id)/{filegroup:inputs|files}/(filename)*

            * - not required

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
        _id = kwargs.get('_id')
        analysis = self.storage.get_container(_id)
        filegroup = kwargs.get('filegroup')
        filename = kwargs.get('filename')

        parent = self.storage.get_parent(_id, cont=analysis)
        cid = analysis['parent']['id']
        cont_name = analysis['parent']['type']
        permchecker = self.get_permchecker(parent)

        ticket_id = self.get_param('ticket')
        ticket = None
        if ticket_id is None:
            permchecker(noop)('GET')
        elif ticket_id != '':
            ticket = self._check_ticket(ticket_id, cid, filename)
            if not self.origin.get('id'):
                self.origin = ticket.get('origin')

        if filename:
            # Allow individual file lookups to just specify `files`
            fileinfo = analysis.get('inputs', []) + analysis.get('files',[])
            fileinfo = [fi for fi in fileinfo if fi['name'] == filename]
        else:
            fileinfo = analysis.get(filegroup, [])

        if not fileinfo:
            error_msg = 'No files on analysis {}'.format(_id)
            if filename:
                error_msg = 'Could not find file {} on analysis {}'.format(filename, _id)
            self.abort(404, error_msg)
        if ticket_id == '':
            if filename:
                total_size = fileinfo[0]['size']
                file_cnt = 1
                ticket = util.download_ticket(self.request.client_addr, self.origin, 'file', cid, filename, total_size)
            else:
                targets, total_size, file_cnt = self._prepare_batch(filegroup, fileinfo, analysis)
                label = util.sanitize_string_to_filename(analysis.get('label', 'No Label'))
                filename = 'analysis_' + label + '.tar'
                ticket = util.download_ticket(self.request.client_addr, self.origin, 'batch', targets, filename, total_size)
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
                            self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid, filename=fileinfo['name'])
                            config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
                    else:
                        self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid, filename=fileinfo['name'])

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
                    self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid, filename=fileinfo['name'])
                    config.db.downloads.update_one({'_id': ticket_id}, {'$set': {'logged': True}})
            else:
                self.log_user_access(AccessType.download_file, cont_name=cont_name, cont_id=cid, filename=fileinfo['name'])


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


    def _prepare_batch(self, filegroup, fileinfo, analysis):
        ## duplicated code from download.py
        ## we need a way to avoid this
        targets = []
        total_size = total_cnt = 0
        data_path = config.get_item('persistent', 'data_path')
        dirname = 'input' if filegroup == 'inputs' else 'output'
        for f in fileinfo:
            filepath = os.path.join(data_path, util.path_from_hash(f['hash']))
            if os.path.exists(filepath): # silently skip missing files
                targets.append((filepath,
                                '/'.join([util.sanitize_string_to_filename(analysis['label']), dirname, f['name']]),
                                'analyses', analysis['_id'], f['size']))
                total_size += f['size']
                total_cnt += 1
        return targets, total_size, total_cnt


    def _send_batch(self, ticket):
        self.abort(400, 'This endpoint does not download files, only returns ticket {} for the download'.format(ticket))
