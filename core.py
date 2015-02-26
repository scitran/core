# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('scitran.api')

import os
import re
import hashlib
import tarfile
import markdown
import jsonschema

import base
import util
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
            Resource                                                 | Description
            :--------------------------------------------------------|:-----------------------
            [(api/sites)]                                        | local and remote sites
            api/upload                                           | upload
            api/download                                         | download
            [(api/log)]                                          | log messages
            [(api/search)]                                       | search
            [(api/users)]                                        | list of users
            [(api/users/count)]                                  | count of users
            [(api/users/self)]                                   | user identity
            [(api/users/roles)]                                  | user roles
            [(api/users/schema)]                                 | schema for single user
            api/users/*<uid>*                                    | details for user *<uid>*
            [(api/groups)]                                       | list of groups
            [(api/groups/count)]                                 | count of groups
            [(api/groups/schema)]                                | schema for single group
            api/groups/*<gid>*                                   | details for group *<gid>*
            [(api/projects)]                                     | list of projects
            [(api/projects/count)]                               | count of projects
            [(api/projects/groups)]                              | groups for projects
            [(api/projects/schema)]                              | schema for single project
            api/projects/*<pid>*                                 | details for project *<pid>*
            api/projects/*<pid>*/sessions                        | list sessions for project *<pid>*
            [(api/sessions/count)]                               | count of sessions
            [(api/sessions/schema)]                              | schema for single session
            api/sessions/*<sid>*                                 | details for session *<sid>*
            api/sessions/*<sid>*/move                            | move session *<sid>* to a different project
            api/sessions/*<sid>*/acquisitions                    | list acquisitions for session *<sid>*
            [(api/acquisitions/count)]                           | count of acquisitions
            [(api/acquisitions/schema)]                          | schema for single acquisition
            api/acquisitions/*<aid>*                             | details for acquisition *<aid>*
            [(api/collections)]                                  | list of collections
            [(api/collections/count)]                            | count of collections
            [(api/collections/schema)]                           | schema for single collection
            api/collections/*<cid>*                              | details for collection *<cid>*
            api/collections/*<cid>*/sessions                     | list of sessions for collection *<cid>*
            api/collections/*<cid>*/acquisitions?session=*<sid>* | list of acquisitions for collection *<cid>*, optionally restricted to session *<sid>*
            """

        if self.debug and self.uid:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](\1?user=%s)' % self.uid, resources)
        else:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](\1)', resources)
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
        # check permissions of everything
        # allow downloads of permitted items only
        pass

    def _archivestream(self, tkt_spec):
        pass

    def download(self):
        ticket_id = self.request.get('ticket')
        if ticket_id:
            tkt_spec = self.app.db.downloads.find_one({'_id': ticket_id})
            if not tkt_spec:
                self.abort(404, 'no such ticket')
            if tkt_spec['type'] == 'single':
                self.response.app_iter = open(tkt_spec['filepath'], 'rb')
            else:
                self.response.app_iter = self._archivestream(tkt_spec) # returns iterator
            self.response.headers['Content-Type'] = 'application/octet-stream'
            self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(tkt_spec['filename'])
            self.response.headers['Content-Length'] = str(tkt_spec['size']) # must be set after setting app_iter
        else:
            try:
                req_spec = self.request.json_body
                jsonschema.validate(req_spec, DOWNLOAD_SCHEMA)
            except (ValueError, jsonschema.ValidationError) as e:
                self.abort(400, str(e))
            return self._preflight_archivestream(req_spec)

    def sites(self):
        """Return local and remote sites."""
        if self.app.config['site_id'] == 'local':
            return [{'_id': 'local', 'name': 'Local', 'onload': True}]
        if self.public_request or self.request.get('all').lower() in ('1', 'true'):
            sites = list(self.app.db.sites.find(None, ['name']))
        else:
            remote_ids = (self.app.db.users.find_one({'_id': self.uid}, ['remotes']) or {}).get('remotes', []) + [self.app.config['site_id']]
            sites = list(self.app.db.sites.find({'_id': {'$in': remote_ids}}, ['name']))
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
        if self.request.method == 'GET':
            return self.search_schema
        else:
            pass
