import bson
import datetime
import json
import os.path

from . import base
from . import config
from . import files
from . import rules
from . import tempdir as tempfile
from . import placer as pl
from . import util
from . import validators
from .dao import hierarchy, APIStorageException

log = config.log

Strategy = util.Enum('Strategy', {
    'targeted'   : pl.TargetedPlacer,   # Upload N files to a container.
    'engine'     : pl.EnginePlacer,	  # Upload N files from the result of a successful job.
    'packfile'   : pl.PackfilePlacer,	  # Upload N files as a new packfile to a container.
    'labelupload': pl.LabelPlacer,
    'uidupload'  : pl.UIDPlacer,
})

def process_upload(request, strategy, container_type=None, id=None, origin=None):
    """
    Universal file upload entrypoint.

    Format:
        Multipart form upload with N file fields, each with their desired filename.
        For technical reasons, no form field names can be repeated. Instead, use (file1, file2) and so forth.

        Depending on the type of upload, a non-file form field called "metadata" may/must also be sent.
        If present, it is expected to be a JSON string matching the schema for the upload strategy.

        Currently, the JSON returned may vary by strategy.

        Some examples:
        curl -F file1=@science.png   -F file2=@rules.png url
        curl -F metadata=<stuff.json -F file=@data.zip   url
        http --form POST url metadata=@stuff.json file@data.zip

    Features:
                                               | targeted |  reaper   | engine | packfile
        Must specify a target container        |     X    |           |    X   |
        May create hierarchy on demand         |          |     X     |        |     X

        May  send metadata about the files     |     X    |     X     |    X   |     X
        MUST send metadata about the files     |          |     X     |        |     X

        Creates a packfile from uploaded files |          |           |        |     X
    """

    if not isinstance(strategy, Strategy):
        raise Exception('Unknown upload strategy')

    if id is not None and container_type == None:
        raise Exception('Unspecified container type')

    if container_type is not None and container_type not in ('acquisition', 'session', 'project', 'collection'):
        raise Exception('Unknown container type')

    timestamp = datetime.datetime.utcnow()

    container = None
    if container_type and id:
        container = hierarchy.get_container(container_type, id)

    # The vast majority of this function's wall-clock time is spent here.
    # Tempdir is deleted off disk once out of scope, so let's hold onto this reference.
    form, tempdir = files.process_form(request)

    metadata = None
    if 'metadata' in form:
        # Slight misnomer: the metadata field, if present, is sent as a normal form field, NOT a file form field.
        metadata = json.loads(form['metadata'].file.getvalue())

    placer_class = strategy.value
    placer = placer_class(container_type, container, id, metadata, timestamp, origin)
    placer.check()

    # Browsers, when sending a multipart upload, will send files with field name "file" (if sinuglar)
    # or "file1", "file2", etc (if multiple). Following this convention is probably a good idea.
    # Here, we accept any
    file_fields = filter(lambda x: form[x].filename is not None, form)

    # TODO: Change schemas to enabled targeted uploads of more than one file.
    # Ref docs from placer.TargetedPlacer for details.
    if strategy == Strategy.targeted and len(file_fields) > 1:
        raise Exception("Targeted uploads can only send one file")

    for field in file_fields:
        field = form[field]

        # Augment the cgi.FieldStorage with a variety of custom fields.
        # Not the best practice. Open to improvements.
        # These are presumbed to be required by every function later called with field as a parameter.
        field.path	 = os.path.join(tempdir.name, field.filename)
        field.size	 = os.path.getsize(field.path)
        field.hash	 = field.file.get_formatted_hash()
        field.mimetype = util.guess_mimetype(field.filename) # TODO: does not honor metadata's mime type if any
        field.modified = timestamp

        # create a file-info map commonly used elsewhere in the codebase.
        # Stands in for a dedicated object... for now.
        info = {
            'name':	 field.filename,
            'modified': field.modified, #
            'size':	 field.size,
            'mimetype': field.mimetype,
            'hash':	 field.hash,
            'origin': origin,

            'type': None,
            'instrument': None,
            'measurements': [],
            'tags': [],
            'metadata': {}
        }

        placer.process_file_field(field, info)

    return placer.finalize()


class Upload(base.RequestHandler):

    def reaper(self):
        """Receive a sortable reaper upload."""
        if not self.superuser_request:
            self.abort(402, 'uploads must be from an authorized drone')
        with tempfile.TemporaryDirectory(prefix='.tmp', dir=config.get_item('persistent', 'data_path')) as tempdir_path:
            try:
                file_store = files.FileStore(self.request, tempdir_path)
            except FileStoreException as e:
                self.abort(400, str(e))
            now = datetime.datetime.utcnow()
            fileinfo = dict(
                name=file_store.filename,
                created=now,
                modified=now,
                size=file_store.size,
                hash=file_store.hash,
                mimetype=util.guess_mimetype(file_store.filename),
                tags=file_store.tags,
                metadata=file_store.metadata.get('file', {}).get('metadata', {}),
                origin=self.origin
            )

            target, file_metadata = hierarchy.create_container_hierarchy(file_store.metadata)
            fileinfo.update(file_metadata)
            f = target.find(file_store.filename)
            target_path = os.path.join(config.get_item('persistent', 'data_path'), util.path_from_hash(fileinfo['hash']))
            if not f:
                file_store.move_file(target_path)
                target.add_file(fileinfo)
                rules.create_jobs(config.db, target.container, target.level[:-1], fileinfo)
            elif not files.identical(file_store.hash, file_store.path, f['hash'], util.path_from_hash(f['hash'])):
                file_store.move_file(target_path)
                target.update_file(fileinfo)
                rules.create_jobs(config.db, target.container, target.level[:-1], fileinfo)
            throughput = file_store.size / file_store.duration.total_seconds()
            log.info('Received    %s [%s, %s/s] from %s' % (file_store.filename, util.hrsize(file_store.size), util.hrsize(throughput), self.request.client_addr))

    def upload(self, strategy):
        """Receive a sortable reaper upload."""
        if not self.superuser_request:
            self.abort(402, 'uploads must be from an authorized drone')

        # TODO: what enum
        if strategy == 'label':
            strategy = Strategy.labelupload
        elif strategy == 'uid':
            strategy = Strategy.uidupload
        else:
            self.abort(500, 'stragegy {} not implemented'.format(strategy))
        return process_upload(self.request, strategy, origin=self.origin)

    def engine(self):
        """
        URL format: api/engine?level=<container_type>&id=<container_id>&job=<job_id>
        """

        if not self.superuser_request:
            self.abort(402, 'uploads must be from an authorized drone')

        level = self.get_param('level')
        if level is None:
            self.abort(404, 'container level is required')

        if level != 'acquisition':
            self.abort(404, 'engine uploads are supported only at the acquisition level')

        acquisition_id = self.get_param('id')
        if not acquisition_id:
            self.abort(404, 'container id is required')

        return upload.process_upload(self.request, upload.Strategy.engine, origin=self.origin)
