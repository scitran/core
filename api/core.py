import os
import re
import cgi
import bson
import json
import logging
import tarfile
import zipfile
import datetime
import markdown
import cStringIO
import validators

from . import base
from . import files
from . import util
from . import config
from .util import log
from .dao import reaperutil
from . import tempdir as tempfile


# silence Markdown library logging
logging.getLogger('MARKDOWN').setLevel(logging.WARNING)


class Core(base.RequestHandler):

    """/api """

    def head(self):
        """Return 200 OK."""
        pass

    def get(self):
        """Return API documentation"""
        resources = """
            Resource                            | Description
            :-----------------------------------|:-----------------------
            [(/sites)]                          | local and remote sites
            /download                           | download
            [(/users)]                          | list of users
            [(/users/self)]                     | user identity
            [(/users/roles)]                    | user roles
            [(/users/*<uid>*)]                  | details for user *<uid>*
            [(/users/*<uid>*/groups)]           | groups for user *<uid>*
            [(/users/*<uid>*/projects)]         | projects for user *<uid>*
            [(/groups)]                         | list of groups
            /groups/*<gid>*                     | details for group *<gid>*
            /groups/*<gid>*/projects            | list of projects for group *<gid>*
            /groups/*<gid>*/sessions            | list of sessions for group *<gid>*
            [(/projects)]                       | list of projects
            [(/projects/groups)]                | groups for projects
            [(/projects/schema)]                | schema for single project
            /projects/*<pid>*                   | details for project *<pid>*
            /projects/*<pid>*/sessions          | list sessions for project *<pid>*
            [(/sessions)]                       | list of sessions
            [(/sessions/schema)]                | schema for single session
            /sessions/*<sid>*                   | details for session *<sid>*
            /sessions/*<sid>*/move              | move session *<sid>* to a different project
            /sessions/*<sid>*/acquisitions      | list acquisitions for session *<sid>*
            [(/acquisitions/schema)]            | schema for single acquisition
            /acquisitions/*<aid>*               | details for acquisition *<aid>*
            [(/collections)]                    | list of collections
            [(/collections/schema)]             | schema for single collection
            /collections/*<cid>*                | details for collection *<cid>*
            /collections/*<cid>*/sessions       | list of sessions for collection *<cid>*
            /collections/*<cid>*/acquisitions   | list of acquisitions for collection *<cid>*
            [(/schema/group)]                   | group schema
            [(/schema/user)]                    | user schema
            """

        if self.debug and self.uid:
            resources = re.sub(r'\[\((.*)\)\]', r'[\1](/api\1?user=%s&root=%r)' % (self.uid, self.superuser_request), resources)
            resources = re.sub(r'(\(.*)\*<uid>\*(.*\))', r'\1%s\2' % self.uid, resources)
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
        if self.debug and not self.get_param('user'):
            self.response.write('<form name="username" action="" method="get">\n')
            self.response.write('Username: <input type="text" name="user">\n')
            self.response.write('Root: <input type="checkbox" name="root" value="1">\n')
            self.response.write('<input type="submit" value="Generate Custom Links">\n')
            self.response.write('</form>\n')
        self.response.write(markdown.markdown(resources, ['extra']))
        self.response.write('</body>\n')
        self.response.write('</html>\n')

    def reaper(self):
        """Receive a sortable reaper upload."""
        if not self.superuser_request:
            self.abort(402, 'uploads must be from an authorized drone')
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=self.app.config['data_path']) as tempdir_path:
            try:
                file_store = files.FileStore(self.request, tempdir_path)
            except files.FileStoreException as e:
                self.abort(400, str(e))
            created = modified = datetime.datetime.now()
            fileinfo = dict(
                name=file_store.filename,
                created=created,
                modified=modified,
                size=file_store.size,
                hash=file_store.hash,
                unprocessed=True,
                tags=file_store.tags,
                metadata=file_store.metadata
            )
            container = reaperutil.create_container_hierarchy(file_store.metadata)
            f = container.find(file_store.filename)
            created = modified = datetime.datetime.utcnow()
            target_path = os.path.join(self.app.config['data_path'], container.path)
            if not f:
                file_store.move_file(target_path)
                container.add_file(fileinfo)
            elif not file_store.identical(os.path.join(target_path, file_store.filename), f['hash']):
                file_store.move_file(target_path)
                container.update_file(fileinfo)
            throughput = file_store.size / file_store.duration.total_seconds()
            log.info('Received    %s [%s, %s/s] from %s' % (file_store.filename, util.hrsize(file_store.size), util.hrsize(throughput), self.request.client_addr))

    def _preflight_archivestream(self, req_spec):
        data_path = self.app.config['data_path']
        arc_prefix = 'sdm'

        def append_targets(targets, container, prefix, total_size, total_cnt):
            prefix = arc_prefix + '/' + prefix
            for f in container.get('files', []):
                if req_spec['optional'] or not f.get('optional', False):
                    filepath = os.path.join(data_path, str(container['_id'])[-3:] + '/' + str(container['_id']), f['name'])
                    if os.path.exists(filepath): # silently skip missing files
                        targets.append((filepath, prefix + '/' + f['name'], f['size']))
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
                project = self.app.db.projects.find_one({'_id': item_id}, ['group', 'label', 'files'])
                prefix = project['group'] + '/' + project['label']
                total_size, file_cnt = append_targets(targets, project, prefix, total_size, file_cnt)
                sessions = self.app.db.sessions.find({'project': item_id}, ['label', 'files'])
                session_dict = {session['_id']: session for session in sessions}
                acquisitions = self.app.db.acquisitions.find({'session': {'$in': session_dict.keys()}}, ['label', 'files', 'session'])
                for session in session_dict.itervalues():
                    session_prefix = prefix + '/' + session.get('label', 'untitled')
                    total_size, file_cnt = append_targets(targets, session, session_prefix, total_size, file_cnt)
                for acq in acquisitions:
                    session = session_dict[acq['session']]
                    acq_prefix = prefix + '/' + session.get('label', 'untitled') + '/' + acq.get('label', 'untitled')
                    total_size, file_cnt = append_targets(targets, acq, acq_prefix, total_size, file_cnt)
            elif item['level'] == 'session':
                session = self.app.db.sessions.find_one({'_id': item_id}, ['project', 'label', 'files'])
                project = self.app.db.projects.find_one({'_id': session['project']}, ['group', 'label'])
                prefix = project['group'] + '/' + project['label'] + '/' + session.get('label', 'untitled')
                total_size, file_cnt = append_targets(targets, session, prefix, total_size, file_cnt)
                acquisitions = self.app.db.acquisitions.find({'session': item_id}, ['label', 'files'])
                for acq in acquisitions:
                    acq_prefix = prefix + '/' + acq.get('label', 'untitled')
                    total_size, file_cnt = append_targets(targets, acq, acq_prefix, total_size, file_cnt)
            elif item['level'] == 'acquisition':
                acq = self.app.db.acquisitions.find_one({'_id': item_id}, ['session', 'label', 'files'])
                session = self.app.db.sessions.find_one({'_id': acq['session']}, ['project', 'label'])
                project = self.app.db.projects.find_one({'_id': session['project']}, ['group', 'label'])
                prefix = project['group'] + '/' + project['label'] + '/' + session.get('label', 'untitled') + '/' + acq.get('label', 'untitled')
                total_size, file_cnt = append_targets(targets, acq, prefix, total_size, file_cnt)
        log.debug(json.dumps(targets, sort_keys=True, indent=4, separators=(',', ': ')))
        filename = 'sdm_' + datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S') + '.tar'
        ticket = util.download_ticket(self.request.client_addr, 'batch', targets, filename, total_size)
        self.app.db.downloads.insert_one(ticket)
        return {'ticket': ticket['_id'], 'file_cnt': file_cnt, 'size': total_size}

    def _archivestream(self, ticket):
        BLOCKSIZE = 512
        CHUNKSIZE = 2**20  # stream files in 1MB chunks
        stream = cStringIO.StringIO()
        with tarfile.open(mode='w|', fileobj=stream) as archive:
            for filepath, arcpath, _ in ticket['target']:
                yield archive.gettarinfo(filepath, arcpath).tobuf()
                with open(filepath, 'rb') as fd:
                    for chunk in iter(lambda: fd.read(CHUNKSIZE), ''):
                        yield chunk
                    if len(chunk) % BLOCKSIZE != 0:
                        yield (BLOCKSIZE - (len(chunk) % BLOCKSIZE)) * b'\0'
        yield stream.getvalue() # get tar stream trailer
        stream.close()

    def _symlinkarchivestream(self, ticket, data_path):
        for filepath, arcpath, _ in ticket['target']:
            t = tarfile.TarInfo(name=arcpath)
            t.type = tarfile.SYMTYPE
            t.linkname = os.path.relpath(filepath, data_path)
            yield t.tobuf()
        stream = cStringIO.StringIO()
        with tarfile.open(mode='w|', fileobj=stream) as archive:
            pass
        yield stream.getvalue() # get tar stream trailer
        stream.close()


    def download(self):
        ticket_id = self.get_param('ticket')
        if ticket_id:
            ticket = self.app.db.downloads.find_one({'_id': ticket_id})
            if not ticket:
                self.abort(404, 'no such ticket')
            if ticket['ip'] != self.request.client_addr:
                self.abort(400, 'ticket not for this source IP')
            if self.get_param('symlinks'):
                self.response.app_iter = self._symlinkarchivestream(ticket, self.app.config['data_path'])
            else:
                self.response.app_iter = self._archivestream(ticket)
            self.response.headers['Content-Type'] = 'application/octet-stream'
            self.response.headers['Content-Disposition'] = 'attachment; filename=' + str(ticket['filename'])
        else:
            req_spec = self.request.json_body
            validator = validators.payload_from_schema_file(self, 'input/download.json')
            validator(req_spec, 'POST')
            log.debug(json.dumps(req_spec, sort_keys=True, indent=4, separators=(',', ': ')))
            return self._preflight_archivestream(req_spec)

    def sites(self):
        """Return local and remote sites."""
        projection = ['name', 'onload']
        # TODO onload for local is true
        if self.public_request or self.is_true('all'):
            sites = list(self.app.db.sites.find(None, projection))
        else:
            # TODO onload based on user prefs
            site_id = config.site_id()
            remotes = (self.app.db.users.find_one({'_id': self.uid}, ['remotes']) or {}).get('remotes', [])
            remote_ids = [r['_id'] for r in remotes] + [site_id]
            sites = list(self.app.db.sites.find({'_id': {'$in': remote_ids}}, projection))
        for s in sites:  # TODO: this for loop will eventually move to public case
            if s['_id'] == site_id:
                s['onload'] = True
                break
        return sites
