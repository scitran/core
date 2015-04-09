# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('scitran.api')

import os
import re
import json
import hashlib
import tarfile
import datetime
import markdown
import jsonschema
import collections
import bson.json_util

import base
import util
import zipstream
import tempdir as tempfile

DOWNLOAD_SCHEMA = {
    '$schema': 'http://json-schema.org/draft-04/schema#',
    'title': 'Download',
    'type': 'object',
    'properties': {
        'optional': {
            'type': 'boolean',
        },
        'nodes': {
            'type': 'array',
            'minItems': 1,
            'items': {
                'type': 'object',
                'properties': {
                    'level': {
                        'type': 'string',
                        'enum': ['project', 'session', 'acquisition'],
                    },
                    '_id': {
                        'type': 'string',
                        'pattern': '^[0-9a-f]{24}$',
                    },
                },
                'required': ['level', '_id'],
                'additionalProperties': False
            },
        },
    },
    'required': ['optional', 'nodes'],
    'additionalProperties': False
}

class Core(base.RequestHandler):

    """/api """

    def head(self):
        """Return 200 OK."""
        pass

    def post(self):
        log.error(self.request.body)

    def get(self):
        """Return API documentation"""
        resources = """
            Resource                            | Description
            :-----------------------------------|:-----------------------
            [(/sites)]                          | local and remote sites
            /upload                             | upload
            /download                           | download
            [(/search)]                         | search
            [(/users)]                          | list of users
            [(/users/count)]                    | count of users
            [(/users/self)]                     | user identity
            [(/users/roles)]                    | user roles
            [(/users/schema)]                   | schema for single user
            /users/*<uid>*                      | details for user *<uid>*
            [(/groups)]                         | list of groups
            [(/groups/count)]                   | count of groups
            [(/groups/schema)]                  | schema for single group
            /groups/*<gid>*                     | details for group *<gid>*
            [(/projects)]                       | list of projects
            [(/projects/count)]                 | count of projects
            [(/projects/groups)]                | groups for projects
            [(/projects/schema)]                | schema for single project
            /projects/*<pid>*                   | details for project *<pid>*
            /projects/*<pid>*/sessions          | list sessions for project *<pid>*
            [(/sessions)]                       | list of sessions
            [(/sessions/count)]                 | count of sessions
            [(/sessions/schema)]                | schema for single session
            /sessions/*<sid>*                   | details for session *<sid>*
            /sessions/*<sid>*/move              | move session *<sid>* to a different project
            /sessions/*<sid>*/acquisitions      | list acquisitions for session *<sid>*
            [(/acquisitions/count)]             | count of acquisitions
            [(/acquisitions/schema)]            | schema for single acquisition
            /acquisitions/*<aid>*               | details for acquisition *<aid>*
            [(/collections)]                    | list of collections
            [(/collections/count)]              | count of collections
            [(/collections/schema)]             | schema for single collection
            /collections/*<cid>*                | details for collection *<cid>*
            /collections/*<cid>*/sessions       | list of sessions for collection *<cid>*
            /collections/*<cid>*/acquisitions   | list of acquisitions for collection *<cid>*
            """

        if self.debug and self.uid:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](/api\1?user=%s)' % self.uid, resources)
        else:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](/api\1)', resources)
        resources = resources.replace('<', '&lt;').replace('>', '&gt;').strip()

        self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        self.response.write('<html>\n')
        self.response.write('<head>\n')
        self.response.write('<title>SciTran API</title>\n')
        self.response.write('<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">\n')
        self.response.write('<style type="text/css">\n')
        self.response.write('table {width:0%; border-width:1px; padding: 0;border-collapse: collapse;}\n')
        self.response.write('table tr {border-top: 1px solid #b8b8b8; background-color: white; margin: 0; padding: 0;}\n')
        self.response.write('table tr:nth-child(2n) {background-color: #f8f8f8;}\n')
        self.response.write('table thead tr :last-child {width:100%;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th {font-weight: bold; border: 1px solid #b8b8b8; background-color: #cdcdcd; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr td {border: 1px solid #b8b8b8; margin: 0; padding: 6px 13px;}\n')
        self.response.write('table tr th :first-child, table tr td :first-child {margin-top: 0;}\n')
        self.response.write('table tr th :last-child, table tr td :last-child {margin-bottom: 0;}\n')
        self.response.write('</style>\n')
        self.response.write('</head>\n')
        self.response.write('<body style="min-width:900px">\n')
        if self.debug and not self.request.get('user', None):
            self.response.write('<form name="username" action="" method="get">\n')
            self.response.write('Username: <input type="text" name="user">\n')
            self.response.write('<input type="submit" value="Generate Custom Links">\n')
            self.response.write('</form>\n')
        self.response.write(markdown.markdown(resources, ['extra']))
        self.response.write('</body>\n')
        self.response.write('</html>\n')

    def put(self):
        """Receive a sortable reaper or user upload."""
        #if not self.uid and not self.drone_request:
        #    self.abort(402, 'uploads must be from an authorized user or drone')
        if 'Content-MD5' not in self.request.headers:
            self.abort(400, 'Request must contain a valid "Content-MD5" header.')
        filename = self.request.get('filename', 'upload')
        data_path = self.app.config['data_path']
        quarantine_path = self.app.config['quarantine_path']
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=data_path) as tempdir_path:
            hash_ = hashlib.sha1()
            filepath = os.path.join(tempdir_path, filename)
            with open(filepath, 'wb') as fd:
                for chunk in iter(lambda: self.request.body_file.read(2**20), ''):
                    hash_.update(chunk)
                    fd.write(chunk)
            if hash_.hexdigest() != self.request.headers['Content-MD5']:
                self.abort(400, 'Content-MD5 mismatch.')
            if not tarfile.is_tarfile(filepath):
                self.abort(415, 'Only tar files are accepted.')
            log.info('Received    %s [%s] from %s' % (filename, util.hrsize(self.request.content_length), self.request.user_agent))
            status, detail = util.insert_file(self.app.db.acquisitions, None, None, filepath, hash_.hexdigest(), data_path, quarantine_path)
            if status != 200:
                self.abort(status, detail)

    def _preflight_archivestream(self, req_spec):
        data_path = self.app.config['data_path']
        arc_prefix = 'sdm'

        def append_targets(targets, container, prefix, total_size, total_cnt):
            prefix = arc_prefix + '/' + prefix
            for f in container['files']:
                if req_spec['optional'] or not f.get('optional', False):
                    filename = f['name'] + f['ext']
                    filepath = os.path.join(data_path, str(container['_id'])[-3:] + '/' + str(container['_id']), filename)
                    if os.path.exists(filepath): # silently skip missing files
                        targets.append((filepath, prefix + '/' + filename, f['size']))
                        total_size += f['size']
                        total_cnt += 1
            return total_size, total_cnt

        file_cnt = 0
        total_size = 0
        targets = []
        # FIXME: check permissions of everything
        for item in req_spec['nodes']:
            item_id = bson.ObjectId(item['_id'])
            if item['level'] == 'project':
                project = self.app.db.projects.find_one({'_id': item_id}, ['group_id', 'name', 'files'])
                prefix = project['group_id'] + '/' + project['name']
                total_size, file_cnt = append_targets(targets, project, prefix, total_size, file_cnt)
                sessions = self.app.db.sessions.find({'project': item_id}, ['label', 'files'])
                for session in sessions:
                    session_prefix = prefix + '/' + session.get('label', 'untitled')
                    total_size, file_cnt = append_targets(targets, session, session_prefix, total_size, file_cnt)
                    acquisitions = self.app.db.acquisitions.find({'session': session['_id']}, ['label', 'files'])
                    for acq in acquisitions:
                        acq_prefix = session_prefix + '/' + acq.get('label', 'untitled')
                        total_size, file_cnt = append_targets(targets, acq, acq_prefix, total_size, file_cnt)
            elif item['level'] == 'session':
                session = self.app.db.sessions.find_one({'_id': item_id}, ['project', 'label', 'files'])
                project = self.app.db.projects.find_one({'_id': session['project']}, ['group_id', 'name'])
                prefix = project['group_id'] + '/' + project['name'] + '/' + session.get('label', 'untitled')
                total_size, file_cnt = append_targets(targets, session, prefix, total_size, file_cnt)
                acquisitions = self.app.db.acquisitions.find({'session': item_id}, ['label', 'files'])
                for acq in acquisitions:
                    acq_prefix = prefix + '/' + acq.get('label', 'untitled')
                    total_size, file_cnt = append_targets(targets, acq, acq_prefix, total_size, file_cnt)
            elif item['level'] == 'acquisition':
                acq = self.app.db.acquisitions.find_one({'_id': item_id}, ['session', 'label', 'files'])
                session = self.app.db.sessions.find_one({'_id': acq['session']}, ['project', 'label'])
                project = self.app.db.projects.find_one({'_id': session['project']}, ['group_id', 'name'])
                prefix = project['group_id'] + '/' + project['name'] + '/' + session.get('label', 'untitled') + '/' + acq.get('label', 'untitled')
                total_size, file_cnt = append_targets(targets, acq, prefix, total_size, file_cnt)
        log.debug(json.dumps(targets, sort_keys=True, indent=4, separators=(',', ': ')))
        filename = 'sdm_' + datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S') + '.zip'
        ticket = util.download_ticket('batch', targets, filename, total_size)
        tkt_id = self.app.db.downloads.insert(ticket)
        return {'url': self.uri_for('named_download', fn=filename, _scheme='https', ticket=tkt_id), 'file_cnt': file_cnt, 'size': total_size}

    def _archivestream(self, ticket):
        length = None # FIXME compute actual length
        z = zipstream.ZipFile(allowZip64=True)
        targets = ticket['target']
        for filepath, acrpath, _ in targets:
            z.write(filepath, acrpath)
        return z, length

    def download(self, fn=None):
        # discard the filename, fn. backend doesn't need it, but
        # front end makes uses of filename in url.
        ticket_id = self.request.get('ticket')
        # TODO: what's the default? stream or attach?
        attach = self.request.get('attach').lower() in ['1', 'true']
        if ticket_id:
            ticket = self.app.db.downloads.find_one({'_id': ticket_id})
            if not ticket:
                self.abort(404, 'no such ticket')
            if ticket['type'] == 'single':
                self.response.app_iter = open(ticket['target'], 'rb')
                self.response.headers['Content-Length'] = str(ticket['size']) # must be set after setting app_iter
            else:
                self.response.app_iter, length = self._archivestream(ticket)
                self.response.headers['Content-Length'] = str(length) # must be set after setting app_iter
            if attach:
                self.response.headers['Content-Type'] = 'application/octet-stream'
                self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(ticket['filename'])
            else:
                self.response.headers['Content-Type'] = util.guess_mime(ticket['filename'])
        else:
            try:
                req_spec = self.request.json_body
                jsonschema.validate(req_spec, DOWNLOAD_SCHEMA)
            except (ValueError, jsonschema.ValidationError) as e:
                self.abort(400, str(e))
            log.debug(json.dumps(req_spec, sort_keys=True, indent=4, separators=(',', ': ')))
            return self._preflight_archivestream(req_spec)

    def sites(self):
        """Return local and remote sites."""
        projection = ['name', 'onload']
        # TODO onload for local is true
        if self.public_request or self.request.get('all').lower() in ('1', 'true'):
            sites = list(self.app.db.sites.find(None, projection))
        else:
            # TODO onload based on user prefs
            remote_ids = (self.app.db.users.find_one({'_id': self.uid}, ['remotes']) or {}).get('remotes', []) + [self.app.config['site_id']]
            sites = list(self.app.db.sites.find({'_id': {'$in': remote_ids}}, projection))
        for s in sites:  # TODO: this for loop will eventually move to public case
            if s['_id'] == self.app.config['site_id']:
                s['onload'] = True
                break
        return sites

    search_schema = {
        'title': 'Search',
        'type': 'array',
        'items': [
            {
                'title': 'Session',
                'type': 'array',
                'items': [
                    {
                        'title': 'Date',
                        'type': 'date',
                        'field': 'session.date',
                    },
                    {
                        'title': 'Subject',
                        'type': 'array',
                        'items': [
                            {
                                'title': 'Name',
                                'type': 'array',
                                'items': [
                                    {
                                        'title': 'First',
                                        'type': 'string',
                                        'field': 'session.subject.firstname',
                                    },
                                    {
                                        'title': 'Last',
                                        'type': 'string',
                                        'field': 'session.subject.lastname',
                                    },
                                ],
                            },
                            {
                                'title': 'Date of Birth',
                                'type': 'date',
                                'field': 'session.subject.dob',
                            },
                            {
                                'title': 'Sex',
                                'type': 'string',
                                'enum': ['male', 'female'],
                                'field': 'session.subject.sex',
                            },
                        ],
                    },
                ],
            },
            {
                'title': 'MR',
                'type': 'array',
                'items': [
                    {
                        'title': 'Scan Type',
                        'type': 'string',
                        'enum': ['anatomical', 'fMRI', 'DTI'],
                        'field': 'acquisition.type',
                    },
                    {
                        'title': 'Echo Time',
                        'type': 'number',
                        'field': 'acquisition.echo_time',
                    },
                    {
                        'title': 'Size',
                        'type': 'array',
                        'items': [
                            {
                                'title': 'X',
                                'type': 'integer',
                                'field': 'acquisition.size.x',
                            },
                            {
                                'title': 'Y',
                                'type': 'integer',
                                'field': 'acquisition.size.y',
                            },
                        ],
                    },
                ],
            },
            {
                'title': 'EEG',
                'type': 'array',
                'items': [
                    {
                        'title': 'Electrode Count',
                        'type': 'integer',
                        'field': 'acquisition.electrode_count',
                    },
                ],
            },
        ],
    }

    def search(self):
        """Search."""
        SEARCH_POST_SCHEMA = {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'title': 'File',
            'type': 'object',
            'properties': {
                'subj_code': {
                    'title': 'Subject Code',
                    'type': 'string',
                },
                'subj_firstname': {
                    'title': 'Subject First Name',   # hash
                    'type': 'string',
                },
                'subj_lastname': {
                    'title': 'Subject Last Name',
                    'type': 'string',
                },
                'scan_type': {  # MR SPECIFIC!!!
                    'title': 'Scan Type',
                    'enum': self.app.db.acquisitions.distinct('types.kind')
                },
                'date_from': {
                    'title': 'Date From',
                    'type': 'string',
                },
                'date_to': {
                    'title': 'Date To',
                    'type': 'string',
                },
                'psd': {  # MR SPECIFIC!!!
                    'title': 'PSD Name',
                    'type': 'string',   # 'enum': self.app.db.acquisitions.distinct('psd'),
                },
                'subj_age_max': {  # age in years
                    'title': 'Subject Age Max',
                    'type': 'integer',
                },
                'subj_age_min': {  # age in years
                    'title': 'Subject Age Min',
                    'type': 'integer',
                },
                'exam': {
                    'title': 'Exam Number',
                    'type': 'integer',
                },
                'description': {
                    'title': 'Description',
                    'type': 'string',
                },
            },
            # 'required': ['subj_code', 'scan_type', 'date_from', 'date_to', 'psd_name', 'operator', 'subj_age_max', 'subj_age_min', 'exam'],
            # 'additionalProperties': False
        }
        if self.request.method == 'GET':
            return SEARCH_POST_SCHEMA
        try:
            json_body = self.request.json_body
            jsonschema.validate(json_body, SEARCH_POST_SCHEMA)
        except (ValueError, jsonschema.ValidationError) as e:
            self.abort(400, str(e))

        # TODO: search needs to include operator details? do types of datasets have an 'operator'?
        # TODO: provide a schema that allows directly using the request data, rather than
        # requiring construction of the queries....
        session_query = {}
        exam = json_body.get('exam')
        subj_code = json_body.get('subj_code')
        age_max = json_body.get('subj_age_max')
        age_min = json_body.get('subj_age_min')
        if exam:
            session_query.update({'exam': exam})
        if subj_code:
            session_query.update({'subject.code': subj_code})
        if age_min and age_max:
            session_query.update({'subject.age': {'$gte': age_min, '$lte': age_max}})
        elif age_max:
            session_query.update({'subject.age': {'$lte': age_max}})
        elif age_min:
            session_query.update({'subject.age': {'$gte': age_min}})

        # TODO: don't build these, want to get as close to dump the data from the request
        acq_query = {}
        psd = json_body.get('psd')
        types_kind = json_body.get('scan_type')
        time_fmt = '%Y-%m-%d'  # assume that dates will come in as "2014-01-01"
        description = json_body.get('description')
        date_to = json_body.get('date_to')  # need to do some datetime conversion
        if date_to:
            date_to = datetime.datetime.strptime(date_to, time_fmt)
        date_from = json_body.get('date_from')      # need to do some datetime conversion
        if date_from:
            date_from = datetime.datetime.strptime(date_from, time_fmt)
        if psd:
            acq_query.update({'psd': psd})
        if types_kind:
            acq_query.update({'types.kind': types_kind})
        if date_to and date_from:
            acq_query.update({'timestamp': {'$gte': date_from, '$lte': date_to}})
        elif date_to:
            acq_query.update({'timestamp': {'$lte': date_to}})
        elif date_from:
            acq_query.update({'timestamp': {'$gte': date_from}})
        if description:
            # glob style matching, whole word must exist within description
            pass

        # also query sessions
        # permissions exist at the session level, which will limit the acquisition queries to sessions user has access to
        if not self.superuser_request:
            session_query['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
            acq_query['permissions'] = {'$elemMatch': {'_id': self.uid, 'site': self.source_site}}
        sessions = list(self.app.db.sessions.find(session_query))
        session_ids = [s['_id'] for s in sessions]
        # first find the acquisitions that meet the acquisition level query params
        aquery = {'session': {'$in': session_ids}}
        aquery.update(acq_query)

        # build a more complex response, and clean out database specifics
        groups = []
        projects = []
        sessions = []
        acqs = list(self.app.db.acquisitions.find(aquery))
        for acq in acqs:
            session = self.app.db.sessions.find_one({'_id': acq['session']})
            project = self.app.db.projects.find_one({'_id': session['project']})
            group = project['group_id']
            del project['group_id']
            project['group'] = group
            acq['_id'] = str(acq['_id'])
            acq['session'] = str(acq['session'])
            session['_id'] = str(session['_id'])
            session['project'] = str(session['project'])
            project['_id'] = str(project['_id'])
            if session not in sessions:
                sessions.append(session)
            if project not in projects:
                projects.append(project)
            if group not in groups:
                groups.append(group)

        results = {
            'groups': groups,
            'projects': projects,
            'sessions': sessions,
            'acquisitions': acqs,
        }

        return results
