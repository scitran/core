# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('nimsapi')

import os
import re
import copy
import shutil
import difflib
import hashlib
import tarfile
import datetime
import markdown

import nimsdata

import base
import tempdir as tempfile

def hrsize(size):
    if size < 1000:
        return '%d%s' % (size, 'B')
    for suffix in 'KMGTPEZY':
        size /= 1024.
        if size < 10.:
            return '%.1f%s' % (size, suffix)
        if size < 1000.:
            return '%.0f%s' % (size, suffix)
    return '%.0f%s' % (size, 'Y')


def sort_file(db, filepath, digest, store_path):
    filename = os.path.basename(filepath)
    try:
        log.info('Parsing     %s' % filename)
        dataset = nimsdata.parse(filepath)
    except nimsdata.NIMSDataError:
        return 415, 'Can\'t parse %s' % filename
    else:
        log.info('Sorting     %s' % filename)
        # TODO: bail, if any required params are not set
        acquisition_spec, acquisition_id = update_db(db, dataset)
        acquisition_path = os.path.join(store_path, acquisition_id[-3:] + '/' + acquisition_id)
        if not os.path.exists(acquisition_path):
            os.makedirs(acquisition_path)
        file_spec = copy.deepcopy(acquisition_spec)
        file_spec['files'] = {'$elemMatch': {'type': dataset.nims_file_type, 'state': dataset.nims_file_state, 'kinds': dataset.nims_file_kinds}}
        file_info = dict(
                name=dataset.nims_file_name,
                ext=dataset.nims_file_ext,
                size=os.path.getsize(filepath),
                sha1=digest,
                #hash=dataset.nims_hash, TODO: datasets should be able to hash themselves (but not here)
                type=dataset.nims_file_type,
                kinds=dataset.nims_file_kinds,
                state=dataset.nims_file_state,
                )
        success = db.acquisitions.update(file_spec, {'$set': {'files.$': file_info}})
        if not success['updatedExisting']:
            db.acquisitions.update(acquisition_spec, {'$push': {'files': file_info}})
        shutil.move(filepath, acquisition_path + '/' + dataset.nims_file_name + dataset.nims_file_ext)
        log.debug('Done        %s' % filename)
        return 200, 'Success'


def update_db(db, dataset):
    existing_group_ids = [g['_id'] for g in db.groups.find(None, ['_id'])]
    group_id_matches = difflib.get_close_matches(dataset.nims_group_id, existing_group_ids, cutoff=0.8)
    if len(group_id_matches) == 1:
        group_id = group_id_matches[0]
        project_name = dataset.nims_project or 'untitled'
    else:
        group_id = 'unknown'
        project_name = dataset.nims_group_id + ('/' + dataset.nims_project if dataset.nims_project else '')
    group = db.groups.find_one({'_id': group_id})
    project_spec = {'group': group['_id'], 'name': project_name}
    project = db.projects.find_and_modify(
            project_spec,
            {'$setOnInsert': dict(group_name=group.get('name'), permissions=group['roles'], public=False, files=[])},
            upsert=True,
            new=True,
            )
    # project timestamp and timezone are inherited from latest acquisition
    if dataset.nims_metadata_status and project.get('timestamp', dataset.nims_timestamp - datetime.timedelta(1)) < dataset.nims_timestamp:
        db.projects.update(project_spec, {'$set': dict(timestamp=dataset.nims_timestamp, timezone=dataset.nims_timezone)})
    session_spec = {'uid': dataset.nims_session_id}
    session = db.sessions.find_and_modify(
            session_spec,
            {
                '$setOnInsert': dict(project=project['_id'], permissions=project['permissions'], public=project['public'], files=[]),
                '$set': entity_metadata(dataset, dataset.session_properties, session_spec), # session_spec ensures non-empty $set
                '$addToSet': {'domains': dataset.nims_file_domain},
                },
            upsert=True,
            new=True,
            )
    # session timestamp and timezone are inherited from earliest acquisition
    if dataset.nims_metadata_status and session.get('timestamp', dataset.nims_timestamp + datetime.timedelta(1)) > dataset.nims_timestamp:
        db.sessions.update(session_spec, {'$set': dict(timestamp=dataset.nims_timestamp, timezone=dataset.nims_timezone)})
    acquisition_spec = {'uid': dataset.nims_acquisition_id}
    acquisition = db.acquisitions.find_and_modify(
            acquisition_spec,
            {
                '$setOnInsert': dict(session=session['_id'], permissions=session['permissions'], public=project['public'], files=[]),
                '$set': entity_metadata(dataset, dataset.acquisition_properties, acquisition_spec), # acquisition_spec ensures non-empty $set
                '$addToSet': {'types': {'$each': [{'domain': dataset.nims_file_domain, 'kind': kind} for kind in dataset.nims_file_kinds]}},
                },
            upsert=True,
            new=True,
            )
    return acquisition_spec, str(acquisition['_id'])


def entity_metadata(dataset, properties, metadata={}, parent_key=''):
    metadata = copy.deepcopy(metadata)
    if dataset.nims_metadata_status is not None:
        parent_key = parent_key and parent_key + '.'
        for key, attributes in properties.iteritems():
            if attributes['type'] == 'object':
                metadata.update(entity_metadata(dataset, attributes['properties'], parent_key=key))
            else:
                value = getattr(dataset, attributes['field']) if 'field' in attributes else None
                if value or value == 0: # drop Nones and empty iterables
                    metadata[parent_key + key] = value
    return metadata


class Core(base.RequestHandler):

    """/nimsapi """

    def head(self):
        """Return 200 OK."""
        self.response.set_status(200)

    def get(self):
        """Return API documentation"""
        resources = """
            Resource                                                 | Description
            :--------------------------------------------------------|:-----------------------
            nimsapi/login                                            | user login
            [(nimsapi/sites)]                                        | local and remote sites
            [(nimsapi/roles)]                                        | user roles
            nimsapi/upload                                           | upload
            nimsapi/download                                         | download
            [(nimsapi/log)]                                          | log messages
            [(nimsapi/search)]                                       | search
            [(nimsapi/users)]                                        | list of users
            [(nimsapi/users/count)]                                  | count of users
            [(nimsapi/users/schema)]                                 | schema for single user
            nimsapi/users/*<uid>*                                    | details for user *<uid>*
            [(nimsapi/groups)]                                       | list of groups
            [(nimsapi/groups/count)]                                 | count of groups
            [(nimsapi/groups/schema)]                                | schema for single group
            nimsapi/groups/*<gid>*                                   | details for group *<gid>*
            [(nimsapi/projects)]                                     | list of projects
            [(nimsapi/projects/count)]                               | count of projects
            [(nimsapi/projects/schema)]                              | schema for single project
            nimsapi/projects/*<pid>*                                 | details for project *<pid>*
            nimsapi/projects/*<pid>*/sessions                        | list sessions for project *<pid>*
            [(nimsapi/sessions/count)]                               | count of sessions
            [(nimsapi/sessions/schema)]                              | schema for single session
            nimsapi/sessions/*<sid>*                                 | details for session *<sid>*
            nimsapi/sessions/*<sid>*/move                            | move session *<sid>* to a different project
            nimsapi/sessions/*<sid>*/acquisitions                    | list acquisitions for session *<sid>*
            [(nimsapi/acquisitions/count)]                           | count of acquisitions
            [(nimsapi/acquisitions/schema)]                          | schema for single acquisition
            nimsapi/acquisitions/*<aid>*                             | details for acquisition *<aid>*
            [(nimsapi/collections)]                                  | list of collections
            [(nimsapi/collections/count)]                            | count of collections
            [(nimsapi/collections/schema)]                           | schema for single collection
            nimsapi/collections/*<cid>*                              | details for collection *<cid>*
            nimsapi/collections/*<cid>*/sessions                     | list of sessions for collection *<cid>*
            nimsapi/collections/*<cid>*/acquisitions?session=*<sid>* | list of acquisitions for collection *<cid>*, optionally restricted to session *<sid>*
            """

        if self.debug and self.uid:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](\1?user=%s)' % self.uid, resources)
        else:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](\1)', resources)
        resources = resources.replace('<', '&lt;').replace('>', '&gt;').strip()

        self.response.headers['Content-Type'] = 'text/html; charset=utf-8'
        self.response.write('<html>\n')
        self.response.write('<head>\n')
        self.response.write('<title>NIMSAPI</title>\n')
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
        # TODO add security: either authenticated user or machine-to-machine CRAM
        if 'Content-MD5' not in self.request.headers:
            self.abort(400, 'Request must contain a valid "Content-MD5" header.')
        filename = self.request.get('filename', 'upload')
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=self.app.config['store_path']) as tempdir_path:
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
            log.info('Received    %s [%s] from %s' % (filename, hrsize(self.request.content_length), self.request.user_agent))
            status, detail = sort_file(self.app.db, filepath, hash_.hexdigest(), self.app.config['store_path'])
            if status != 200:
                self.abort(status, detail)

    def download(self):
        if self.request.method == 'OPTIONS':
            return self.options()
        paths = []
        symlinks = []
        for js_id in self.request.get('id', allow_multiple=True):
            type_, _id = js_id.split('_')
            _idpaths, _idsymlinks = resource_types[type_].download_info(_id)
            paths += _idpaths
            symlinks += _idsymlinks

    def login(self):
        """Return details for the current User."""
        if self.request.method == 'OPTIONS':
            return self.options()
        #if self.uid is not None:
        log.debug(self.uid + ' has logged in')
        return self.app.db.users.find_and_modify({'_id': self.uid}, {'$inc': {'logins': 1}}, fields=['firstname', 'lastname', 'superuser'])

    def sites(self):
        """Return local and remote sites."""
        if self.request.method == 'OPTIONS':
            return self.options()
        if self.request.get('all').lower() in ('1', 'true'):
            remotes = list(self.app.db.remotes.find(None, ['name']))
        else:
            remotes = (self.app.db.users.find_one({'_id': self.uid}, ['remotes']) or {}).get('remotes', [])
        return dict(local={'_id': self.app.config['site_id'], 'name': self.app.config['site_name']}, remotes=remotes)

    def roles(self):
        """Return the list of user roles."""
        if self.request.method == 'OPTIONS':
            return self.options()
        return base.ROLES

    def log(self):
        """Return logs."""
        if self.request.method == 'OPTIONS':
            return self.options()
        try:
            logs = open(self.app.config['log_path']).readlines()
        except IOError as e:
            if 'Permission denied' in e:
                body_template = '${explanation}<br /><br />${detail}<br /><br />${comment}'
                comment = 'To fix permissions, run the following command: chmod o+r ' + self.app.config['log_path']
                self.abort(500, detail=str(e), comment=comment, body_template=body_template)
            else: # file does not exist
                self.abort(500, 'log_path variable misconfigured or not set')
        try:
            n = int(self.request.get('n', 10000))
        except:
            self.abort(400, 'n must be an integer')
        return [line.strip() for line in reversed(logs) if re.match('[-:0-9 ]{18} +nimsapi:(?!.*[/a-z]*/log )', line)][:n]

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
        if self.request.method == 'OPTIONS':
            return self.options()
        elif self.request.method == 'GET':
            return self.search_schema
        else:
            pass
