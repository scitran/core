# @author:  Gunnar Schaefer, Kevin S. Hahn

import logging
log = logging.getLogger('nimsapi')

import os
import re
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


class Core(base.RequestHandler):

    """/nimsapi """

    def head(self):
        """Return 200 OK."""
        self.response.set_status(200)

    def get(self):
        """Return API documentation"""
        resources = """
            Resource                                            | Description
            :---------------------------------------------------|:-----------------------
            nimsapi/login                                       | user login
            [(nimsapi/sites)]                                   | local and remote sites
            [(nimsapi/roles)]                                   | user roles
            nimsapi/upload                                      | upload
            nimsapi/download                                    | download
            [(nimsapi/log)]                                     | log messages
            [(nimsapi/users)]                                   | list of users
            [(nimsapi/users/count)]                             | count of users
            [(nimsapi/users/listschema)]                        | schema for user list
            [(nimsapi/users/schema)]                            | schema for single user
            nimsapi/users/*<uid>*                               | details for user *<uid>*
            [(nimsapi/groups)]                                  | list of groups
            [(nimsapi/groups/count)]                            | count of groups
            [(nimsapi/groups/listschema)]                       | schema for group list
            [(nimsapi/groups/schema)]                           | schema for single group
            nimsapi/groups/*<gid>*                              | details for group *<gid>*
            [(nimsapi/experiments)]                             | list of experiments
            [(nimsapi/experiments/count)]                       | count of experiments
            [(nimsapi/experiments/listschema)]                  | schema for experiment list
            [(nimsapi/experiments/schema)]                      | schema for single experiment
            nimsapi/experiments/*<xid>*                         | details for experiment *<xid>*
            nimsapi/experiments/*<xid>*/sessions                | list sessions for experiment *<xid>*
            [(nimsapi/sessions/count)]                          | count of sessions
            [(nimsapi/sessions/listschema)]                     | schema for sessions list
            [(nimsapi/sessions/schema)]                         | schema for single session
            nimsapi/sessions/*<sid>*                            | details for session *<sid>*
            nimsapi/sessions/*<sid>*/move                       | move session *<sid>* to a different experiment
            nimsapi/sessions/*<sid>*/epochs                     | list epochs for session *<sid>*
            [(nimsapi/epochs/count)]                            | count of epochs
            [(nimsapi/epochs/listschema)]                       | schema for epoch list
            [(nimsapi/epochs/schema)]                           | schema for single epoch
            nimsapi/epochs/*<eid>*                              | details for epoch *<eid>*
            [(nimsapi/collections)]                             | list of collections
            [(nimsapi/collections/count)]                       | count of collections
            [(nimsapi/collections/listschema)]                  | schema for collections list
            [(nimsapi/collections/schema)]                      | schema for single collection
            nimsapi/collections/*<cid>*                         | details for collection *<cid>*
            nimsapi/collections/*<cid>*/sessions                | list sessions for collection *<cid>*
            nimsapi/collections/*<cid>*/epochs?session=*<sid>*  | list of epochs for collection *<cid>*, optionally restricted to session *<sid>*
            """

        if self.debug:
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
        filename = self.request.get('filename', 'anonymous')
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
            try:
                log.info('Parsing     %s' % filename)
                dataset = nimsdata.parse(filepath)
            except nimsdata.NIMSDataError:
                self.abort(415, 'Can\'t parse %s' % filename)
            else:
                log.info('Sorting     %s' % filename)
                # TODO: bail, if any required params are not set
                epoch_spec, epoch_id = self.update_db(dataset)
                epoch_path = os.path.join(self.app.config['store_path'], epoch_id[-3:] + '/' + epoch_id)
                if not os.path.exists(epoch_path):
                    os.makedirs(epoch_path)
                file_ext = '.' + filename.split('.', 1)[1] # split on the first .
                file_spec = dict(epoch_spec.items() + [('files.datatype', dataset.nims_type)])
                file_info = dict(
                        datatype=dataset.nims_type,
                        filename=dataset.nims_filename,
                        ext=file_ext,
                        size=os.path.getsize(filepath),
                        sha1=hash_.hexdigest(),
                        #hash=dataset.nims_hash, TODO: datasets should be able to hash themselves (but not here)
                        )
                success = self.app.db.epochs.update(file_spec, {'$set': {'files.$': file_info}})
                if not success['updatedExisting']:
                    self.app.db.epochs.update(epoch_spec, {'$addToSet': {'files': file_info}})
                shutil.move(filepath, epoch_path + '/' + dataset.nims_filename + file_ext)
                log.debug('Done        %s' % filename)

    def update_db(self, dataset):
        existing_group_ids = [g['_id'] for g in self.app.db.groups.find(None, ['_id'])]
        group_id_matches = difflib.get_close_matches(dataset.nims_group, existing_group_ids, cutoff=0.8)
        if len(group_id_matches) == 1:
            group_id = group_id_matches[0]
            experiment_name = dataset.nims_experiment or 'untitled'
        else:
            group_id = 'unknown'
            experiment_name = group_id + '/' + dataset.nims_experiment
        group = self.app.db.groups.find_one({'_id': group_id})
        experiment_spec = {'group': group['_id'], 'name': experiment_name}
        experiment = self.app.db.experiments.find_and_modify(
                experiment_spec,
                {'$setOnInsert': base.NoNoneDict(group_name=group.get('name'), permissions=group['roles'], files=[])},
                upsert=True,
                new=True,
                )
        # experiment timestamp and timezone are inherited from latest epoch
        if experiment.get('timestamp', dataset.nims_timestamp - datetime.timedelta(1)) < dataset.nims_timestamp:
            self.app.db.experiments.update(experiment_spec, {'$set': dict(timestamp=dataset.nims_timestamp, timezone=dataset.nims_timezone)})
        session_spec = {'uid': dataset.nims_session}
        session = self.app.db.sessions.find_and_modify(
                session_spec,
                {
                    '$setOnInsert': dict(experiment=experiment['_id'], files=[]),
                    '$set': self.entity_metadata(dataset, dataset.session_properties, session_spec), # session_spec ensures non-empty $set
                    },
                upsert=True,
                new=True,
                )
        # session timestamp and timezone are inherited from earliest epoch
        if session.get('timestamp', dataset.nims_timestamp + datetime.timedelta(1)) > dataset.nims_timestamp:
            self.app.db.sessions.update(session_spec, {'$set': dict(timestamp=dataset.nims_timestamp, timezone=dataset.nims_timezone)})
        epoch_spec = {'uid': dataset.nims_epoch}
        epoch = self.app.db.epochs.find_and_modify(
                epoch_spec,
                {
                    '$setOnInsert': dict(session=session['_id'], files=[]),
                    '$set': self.entity_metadata(dataset, dataset.epoch_properties),
                    },
                upsert=True,
                new=True,
                )
        return epoch_spec, str(epoch['_id'])

    @staticmethod
    def entity_metadata(dataset, properties, presets={}):
        metadata = [(prop, getattr(dataset, attrs['attribute'])) for prop, attrs in properties.iteritems() if 'attribute' in attrs]
        return base.NoNoneDict(metadata, **presets)

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
        log.debug(self.uid + ' has logged in')
        return self.app.db.users.find_and_modify({'_id': self.uid}, {'$inc': {'logins': 1}}, fields=['firstname', 'lastname', 'superuser'])

    def sites(self):
        """Return local and remote sites."""
        if self.request.method == 'OPTIONS':
            return self.options()
        return dict(
                local={'_id': self.app.config['site_id'], 'name': self.app.config['site_name']},
                remotes=list(self.app.db.remotes.find(None, ['name'])),
                )

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
