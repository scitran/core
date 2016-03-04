import bson
import copy
import datetime
import dateutil
import os
import pymongo
import zipfile

from . import base
from . import config
from . import files
from . import rules
from . import tempdir as tempfile
from . import util
from . import validators
from .dao import reaperutil, APIStorageException


class Placer(object):
    """
    Interface for a placer, which knows how to process files and place them where they belong - on disk and database.
    """

    def __init__(self, container_type, container, id, metadata, timestamp):
        self.container_type = container_type
        self.container	  = container
        self.id			 = id
        self.metadata	   = metadata
        self.timestamp	  = timestamp

    def check(self):
        """
        Run any pre-processing checks. Expected to throw on error.
        """
        raise NotImplementedError()

    def process_file_field(self, field, info):
        """"
        Process a single file field.
        """
        raise NotImplementedError()

    def finalize(self):
        """
        Run any post-processing work. Expected to return output for the callee.
        """
        raise NotImplementedError()

    def requireTarget(self):
        """
        Helper function that throws unless a container was provided.
        """
        if self.id is None or self.container is None or self.container_type is None:
            raise Exception('Must specify a target')

    def requireMetadata(self):
        """
        Helper function that throws unless metadata was provided.
        """
        if self.metadata == None:
            raise Exception('Metadata required')

    def save_file(self, field, info):
        """
        Helper function that moves a file saved via a form field into our CAS.
        May trigger jobs, if applicable, so this should only be called once we're ready for that.

        Requires an augmented file field; see process_upload() for details.
        """

        # Save file
        files.move_form_file_field_into_cas(field)

        # Update the DB
        reaperutil.upsert_fileinfo(self.container_type, self.id, info)

        # Queue any jobs as a result of this upload
        rules.create_jobs(config.db, self.container, self.container_type, info)


class TargetedPlacer(Placer):
    """
    A placer that can accept N files to a specific container (acquisition, etc).

    LIMITATION: To temporarily avoid messing with the JSON schema, this endpoint can only consume one file :(
    An exception is thrown in upload.process_upload() if you try. This could be fixed by making a better schema.
    """

    def check(self):
        self.requireTarget()
        validators.validate_data(self.metadata, 'file.json', 'POST', optional=True)
        self.saved = []

    def process_file_field(self, field, info):
        self.save_file(field, info)
        self.saved.append(info)

    def finalize(self):
        return self.saved


class ReaperPlacer(Placer):
    """
    A placer that can accept files sent to it from a reaper.
    Currently a stub.
    """

    # def check(self):
    # 	self.requireMetadata()

    # def process_file_field(self, field, info):
    # 	pass

    # def finalize(self):
    # 	pass


class EnginePlacer(Placer):
    """
    A placer that can accept files sent to it from an engine.
    Currently a stub.
    """

    def check(self):
        self.requireTarget()
        validators.validate_data(self.metadata, 'enginemetadata.json', 'POST', optional=True)

    def process_file_field(self, field, info):
        if self.metadata is not None:
            # OPPORTUNITY: hard-coded container levels will need to go away soon
            # Engine shouldn't know about container names; maybe parent contexts?
            # How will this play with upload unification? Unify schemas as well?
            file_mds = self.metadata.get('acquisition', {}).get('files', [])

            for file_md in file_mds:
                if file_md['name'] == info['name']:
                    break
            else:
                file_md = None

            for x in ('type', 'instrument', 'measurements', 'tags', 'metadata'):
                info[x] = file_md.get(x) or info[x]

        self.save_file(field, info)

    def finalize(self):
        # Updating various properties of the hierarchy; currently assumes acquisitions; might need fixing for other levels.
        # NOTE: only called in EnginePlacer
        bid = bson.ObjectId(self.id)
        self.obj = reaperutil.update_container_hierarchy(self.metadata, bid, '')

        return {}


class PackfilePlacer(Placer):
    """
    A place that can accept N files, save them into a zip archive, and place the result on an acquisition.
    """

    def check(self):
        self.requireMetadata()
        validators.validate_data(self.metadata, 'packfile.json', 'POST')

        # Save required fields
        self.p_id  = self.metadata['project']['_id']
        self.s_label = self.metadata['session']['label']
        self.a_label = self.metadata['acquisition']['label']

        # Get project info that we need later
        project = config.db['projects'].find_one({ '_id': bson.ObjectId(self.p_id)})
        self.permissions = project.get('permissions', {})
        self.g_id = project['group']

        # If a timestamp was provided, use that for zip files. Otherwise use a set date.
        # Normally we'd use epoch, but zips cannot support years older than 1980, so let's use that instead.
        # Then, given the ISO string, convert it to an epoch integer.
        minimum = datetime.datetime(1980, 1, 1).isoformat()
        stamp   = self.metadata['acquisition'].get('timestamp', minimum)
        self.ziptime = int(dateutil.parser.parse(stamp).strftime('%s'))

        # The zipfile is a santizied acquisition label
        self.dir = util.sanitize_string_to_filename(self.a_label)
        self.name = self.dir + '.zip'

        # Make a tempdir to store zip until moved
        # OPPORTUNITY: this is also called in files.py. Could be a util func.
        self.tempdir = tempfile.TemporaryDirectory(prefix='.tmp', dir=config.get_item('persistent', 'data_path'))

        # Create a zip in the tempdir that later gets moved into the CAS.
        self.path = os.path.join(self.tempdir.name, 'temp.zip')
        self.zip  = zipfile.ZipFile(self.path, 'w', zipfile.ZIP_DEFLATED, allowZip64=True)

        # OPPORTUNITY: add zip comment
        # self.zip.comment = json.dumps(metadata, default=metadata_encoder)

        # Bit of a silly hack: write our tempdir directory into the zip (not including its contents).
        # Creates an empty directory entry in the zip which will hold all the files inside.
        # This way, when you expand a zip, you'll get folder/things instead of a thousand dicoms splattered everywhere.
        self.zip.write(self.tempdir.name, self.dir)

    def process_file_field(self, field, info):
        # Set the file's mtime & atime.
        os.utime(field.path, (self.ziptime, self.ziptime))

        # Place file into the zip folder we created before
        self.zip.write(field.path, os.path.join(self.dir, field.filename))

    def finalize(self):
        self.zip.close()

        # Create an anyonmous object in the style of our augmented file fields.
        # Not a great practice. See process_upload() for details.
        cgi_field = util.obj_from_map({
            'filename': self.name,
            'path':	 self.path,
            'size':	 os.path.getsize(self.path),
            'hash':	 files.hash_file_formatted(self.path),
            'mimetype': util.guess_mimetype('lol.zip'),
            'modified': self.timestamp
        })

        # Similarly, create the info map that is consumed by helper funcs. Clear duplication :(
        # This could be coalesced into a single map thrown on file fields, for example.
        # Used in the API return.
        cgi_info = {
            'name':	 cgi_field.filename,
            'modified': cgi_field.modified,
            'size':	 cgi_field.size,
            'hash':	 cgi_field.hash,

            'type': self.metadata['packfile']['type'],

            # OPPORTUNITY: packfile endpoint could be extended someday to take additional metadata.
            'instrument': None,
            'measurements': [],
            'tags': [],
            'metadata': {}
        }

        # Get or create a session based on the hierarchy and provided labels.
        s = {
            'project': bson.ObjectId(self.p_id),
            'label': self.s_label,
            'group': self.g_id
        }

        # self.permissions

        # Add the subject if one was provided
        new_s = copy.deepcopy(s)
        subject = self.metadata['session'].get('subject')
        if subject is not None:
            new_s['subject'] = subject
        new_s = util.mongo_dict(new_s)

        # Permissions should always be an exact copy
        new_s['permissions'] = self.permissions

        session = config.db['session' + 's'].find_one_and_update(s, {
                '$set': new_s
            },
            upsert=True,
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        # Get or create an acquisition based on the hierarchy and provided labels.
        fields = {
            'session': session['_id'],
            'label': self.a_label
        }

        new_a = copy.deepcopy(fields)
        new_a['permissions'] = self.permissions

        acquisition = config.db['acquisition' + 's'].find_one_and_update(fields, {
                '$set': new_a
            },
            upsert=True,
            return_document=pymongo.collection.ReturnDocument.AFTER
        )

        # Set instance target for helper func
        self.container_type = 'acquisition'
        self.id			 = str(acquisition['_id'])
        self.container	  = acquisition

        self.save_file(cgi_field, cgi_info)

        return {
            'acquisition_id': str(acquisition['_id']),
            'session_id':	 str(session['_id']),
            'info': cgi_info
        }
