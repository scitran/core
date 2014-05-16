#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import logging
import logging.config
log = logging.getLogger('nimsproc.reaper')
logging.getLogger('nimsproc.reaper.scu').setLevel(logging.INFO)     # silence SCU logging
logging.getLogger('requests').setLevel(logging.WARNING)             # silence Requests library logging

import os
import re
import glob
import json
import time
import shutil
import hashlib
import tarfile
import datetime
import requests
import bson.json_util

import scu
import nimsdata
import tempdir as tempfile

DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

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


class Reaper(object):

    def __init__(self, id_, upload_url, pat_id, discard_ids, peripheral_data, sleep_time, tempdir, anonymize):
        self.id_ = id_
        self.upload_url = upload_url
        self.pat_id = pat_id
        self.discard_ids = discard_ids
        self.peripheral_data = peripheral_data
        self.sleep_time = sleep_time
        self.tempdir = tempdir
        self.anonymize = anonymize
        self.datetime_file = os.path.join(os.path.dirname(__file__), '.%s.datetime' % self.id_)
        self.alive = True

    def halt(self):
        self.alive = False

    def get_reference_datetime(self):
        if os.access(self.datetime_file, os.R_OK):
            with open(self.datetime_file, 'r') as f:
                ref_datetime = datetime.datetime.strptime(f.readline(), DATE_FORMAT + '\n')
        else:
            ref_datetime = datetime.datetime.now()
            self.set_reference_datetime(ref_datetime)
        return ref_datetime
    def set_reference_datetime(self, new_datetime):
        with open(self.datetime_file, 'w') as f:
            f.write(new_datetime.strftime(DATE_FORMAT + '\n'))
    reference_datetime = property(get_reference_datetime, set_reference_datetime)

    def retrieve_peripheral_physio(self, name, data_path, reap_path, reap_data, reap_name, log_info):
        lower_time_bound = reap_data.timestamp + datetime.timedelta(seconds=reap_data.prescribed_duration) - datetime.timedelta(seconds=15)
        upper_time_bound = lower_time_bound + datetime.timedelta(seconds=180)
        sleep_time = (upper_time_bound - datetime.datetime.now()).total_seconds()
        if sleep_time > 0:
            log.info('Periph data %s waiting for %s for %d seconds' % (log_info, name, sleep_time))
            time.sleep(sleep_time)

        while True:
            try:
                physio_files = os.listdir(data_path)
            except OSError:
                physio_files = []
            if physio_files:
                break
            else:
                log.warning('Periph data %s %s temporarily unavailable' % (log_info, name))
                time.sleep(5)

        physio_tuples = filter(lambda pt: pt[0], [(re.match('.+_%s_([0-9_]+)' % reap_data.psd_name, pfn), pfn) for pfn in physio_files])
        physio_tuples = [(datetime.datetime.strptime(pts.group(1), '%m%d%Y%H_%M_%S_%f'), pfn) for pts, pfn in physio_tuples]
        physio_tuples = filter(lambda pt: lower_time_bound <= pt[0] <= upper_time_bound, physio_tuples)
        if physio_tuples:
            log.info('Periph data %s %s found' % (log_info, name))
            with tempfile.TemporaryDirectory(dir=self.tempdir) as tempdir_path:
                physio_reap_path = os.path.join(tempdir_path, reap_name)
                os.mkdir(physio_reap_path)
                with open(os.path.join(physio_reap_path, reap_name+'.json'), 'w') as metadata_file:
                    json.dump(nimsdata.nimsphysio.NIMSPhysio.derived_metadata(reap_data), metadata_file, default=bson.json_util.default)
                for pts, pfn in physio_tuples:
                    shutil.copy2(os.path.join(data_path, pfn), physio_reap_path)
                with tarfile.open(os.path.join(reap_path, reap_name+'.tgz'), 'w:gz', compresslevel=6) as archive:
                    archive.add(physio_reap_path, arcname=reap_name)
        else:
            log.info('Periph data %s %s not found' % (log_info, name))

    def retrieve_peripheral_data(self, reap_path, reap_data, reap_name, log_info):
        for pdn, pdp in self.peripheral_data.iteritems():
            if pdn in self.peripheral_data_fn_map:
                self.peripheral_data_fn_map[pdn](self, pdn, pdp, reap_path, reap_data, reap_name+'_'+pdn, log_info)
            else:
                log.warning('Periph data %s %s does not exist' % (log_info, pdn))

    peripheral_data_fn_map = {
            'physio':   retrieve_peripheral_physio
            }

    def upload(self, path, log_info):
        for filename in os.listdir(path):
            filepath = os.path.join(path, filename)
            log.info('Hashing     %s %s' % (log_info, filename))
            hash_ = hashlib.sha1()
            with open(filepath, 'rb') as fd:
                for chunk in iter(lambda: fd.read(1048577 * hash_.block_size), ''):
                    hash_.update(chunk)
            log.info('Uploading   %s %s [%s]' % (log_info, filename, hrsize(os.path.getsize(filepath))))
            with open(filepath, 'rb') as fd:
                headers = {'User-Agent': 'reaper ' + self.id_, 'Content-MD5': hash_.hexdigest()}
                try:
                    start = datetime.datetime.now()
                    r = requests.put(self.upload_url + '?filename=%s_%s' % (self.id_, filename), data=fd, headers=headers)
                    upload_duration = (datetime.datetime.now() - start).total_seconds()
                except requests.exceptions.ConnectionError as e:
                    log.error('Error       %s %s: %s' % (log_info, filename, e))
                    return False
                else:
                    if r.status_code == 200:
                        log.debug('Success     %s %s [%s/s]' % (log_info, filename, hrsize(os.path.getsize(filepath)/upload_duration)))
                    else:
                        log.warning('Failure     %s %s: %s %s' % (log_info, filename, r.status_code, r.reason))
                        return False
        return True


class DicomReaper(Reaper):

    def __init__(self, url, arg_str, pat_id, discard_ids, peripheral_data, sleep_time, tempdir, anonymize):
        self.scu = scu.SCU(*arg_str.split(':'))
        super(DicomReaper, self).__init__(self.scu.aec, url, pat_id, discard_ids, peripheral_data, sleep_time, tempdir, anonymize)

    def run(self):
        monitored_exam = None
        current_exam_datetime = self.reference_datetime
        while self.alive:
            query_params = {
                    'StudyInstanceUID': '',
                    'StudyID': '',
                    'StudyDate': current_exam_datetime.strftime('%Y%m%d-'),
                    'StudyTime': '',
                    'PatientID': '',
                    }
            if self.pat_id:
                query_params['PatientID'] = self.pat_id
            outstanding_exams = [self.Exam(self, scu_resp) for scu_resp in self.scu.find(scu.StudyQuery(**query_params))]
            outstanding_exams = filter(lambda exam: exam.timestamp >= current_exam_datetime, outstanding_exams)
            outstanding_exams.sort(key=lambda exam: exam.timestamp)

            if monitored_exam and outstanding_exams and monitored_exam.id_ != outstanding_exams[0].id_:
                log.warning('Dropping    %s (assumed deleted from scanner)' % monitored_exam)
                monitored_exam = None
                continue

            next_exam = None
            out_ex_cnt = len(outstanding_exams)
            if not monitored_exam and out_ex_cnt > 0:
                next_exam = outstanding_exams[0]
            elif monitored_exam and out_ex_cnt > 1:
                if not any([series.needs_reaping for series in monitored_exam.series_dict.itervalues()]):
                    next_exam = outstanding_exams[1]
                    out_ex_cnt -= 1 # adjust for conditional sleep below

            if next_exam:
                self.reference_datetime = current_exam_datetime = next_exam.timestamp
                if next_exam.pat_id.strip('/').lower() in self.discard_ids:
                    log.info('Discarding  %s' % next_exam)
                    current_exam_datetime += datetime.timedelta(seconds=1)
                    monitored_exam = None
                else:
                    log.info('New         %s' % next_exam)
                    monitored_exam = next_exam

            success = True
            if monitored_exam and self.alive:
                success = monitored_exam.reap()
            if not success or out_ex_cnt < 2: # sleep, if there is a problem or no queue
                time.sleep(self.sleep_time)


    class Exam(object):

        def __init__(self, reaper, scu_resp):
            self.reaper = reaper
            self.id_ = scu_resp.StudyID
            self.uid = scu_resp.StudyInstanceUID
            self.pat_id = scu_resp.PatientID
            self.timestamp = datetime.datetime.strptime(scu_resp.StudyDate + scu_resp.StudyTime[:6], '%Y%m%d%H%M%S')
            self.series_dict = {}

        def __str__(self):
            return 'e%s %s [%s]' % (self.id_, self.timestamp, self.pat_id)

        def reap(self):
            """An exam must be reaped at least twice, since newly encountered series are not immediately reaped."""
            query_params = {
                    'StudyInstanceUID': self.uid,
                    'SeriesInstanceUID': '',
                    'SeriesNumber': '',
                    'SeriesDate': '',
                    'SeriesTime': '',
                    'NumberOfSeriesRelatedInstances': '',
                    }
            success = True
            new_series = {s.uid: s for s in [self.Series(self, scu_resp) for scu_resp in self.reaper.scu.find(scu.SeriesQuery(**query_params))]}
            if new_series:
                for uid in self.series_dict.keys(): # iterate over copy of keys
                    if uid not in new_series:
                        log.info('Dropping    %s (assumed deleted from scanner)' % self.series_dict[uid])
                        del self.series_dict[uid]
            for series in new_series.itervalues():
                if not self.reaper.alive: break
                if series.uid in self.series_dict:
                    success &= self.series_dict[series.uid].reap(series.image_count)
                else:
                    log.info('New         %s' % series)
                    self.series_dict[series.uid] = series
            return success


        class Series(object):

            def __init__(self, exam, scu_resp):
                self.reaper = exam.reaper
                self.exam = exam
                self.id_ = scu_resp.SeriesNumber
                self.uid = scu_resp.SeriesInstanceUID
                if scu_resp.SeriesDate and scu_resp.SeriesTime:
                    self.timestamp = datetime.datetime.strptime(scu_resp.SeriesDate + scu_resp.SeriesTime[:6], '%Y%m%d%H%M%S')
                else:
                    self.timestamp = ''
                self.image_count = int(scu_resp.NumberOfSeriesRelatedInstances)
                self.needs_reaping = True
                self.name_prefix = '%s_%s' % (self.exam.id_, self.id_)
                self.log_info = 'e%s s%s' % (self.exam.id_, self.id_)

            def __str__(self):
                return '%s [%di] %s' % (self.log_info, self.image_count, self.timestamp)

            def reap(self, new_image_count):
                success = False
                if new_image_count != self.image_count:
                    self.image_count = new_image_count
                    self.needs_reaping = True
                    log.info('Monitoring  %s' % self)
                elif self.needs_reaping and self.image_count == 0:
                    self.needs_reaping = False
                    log.warning('Ignoring    %s (zero images)' % self)
                elif self.needs_reaping: # image count has stopped increasing
                    log.info('Reaping     %s' % self)
                    with tempfile.TemporaryDirectory(dir=self.reaper.tempdir) as tempdir_path:
                        reap_count = self.reaper.scu.move(scu.SeriesQuery(StudyInstanceUID='', SeriesInstanceUID=self.uid), tempdir_path)
                        if reap_count == self.image_count:
                            acq_info_dict = self.split_into_acquisitions(tempdir_path)
                            for acq_no, acq_info in acq_info_dict.iteritems():
                                self.reaper.retrieve_peripheral_data(tempdir_path, *acq_info)
                            if self.reaper.upload(tempdir_path, self.log_info):
                                success = True
                                self.needs_reaping = False
                                log.info('Done        %s' % self)
                        else:
                            log.warning('Incomplete  %s, %dr' % (self, reap_count))
                return success

            def split_into_acquisitions(self, series_path):
                if self.reaper.anonymize:
                    log.info('Anonymizing %s' % self)
                dcm_dict = {}
                acq_info_dict = {}
                for filepath in [os.path.join(series_path, filename) for filename in os.listdir(series_path)]:
                    if self.reaper.anonymize:
                        dcm = nimsdata.nimsdicom.NIMSDicom(filepath, metadata_only=False)
                        dcm.write_anonymized_file(filepath)
                    else:
                        dcm = nimsdata.nimsdicom.NIMSDicom(filepath)
                    if os.path.basename(filepath).startswith('(none)'):
                        new_filepath = filepath.replace('(none)', 'NA')
                        os.rename(filepath, new_filepath)
                        filepath = new_filepath
                    os.utime(filepath, (int(dcm.timestamp.strftime('%s')), int(dcm.timestamp.strftime('%s'))))  # correct timestamps
                    if dcm.manufacturer.upper() == 'SIEMENS':
                        dcm_dict.setdefault(1, []).append(filepath) # ignore Siemens acquisition numbers
                    else:
                        dcm_dict.setdefault(dcm.acq_no, []).append(filepath)
                log.info('Compressing %s' % self)
                for acq_no, acq_paths in dcm_dict.iteritems():
                    arcdir_path = os.path.join(series_path, '%s_%s_dicoms' % (self.name_prefix, acq_no))
                    arc_path = '%s.tgz' % arcdir_path
                    dcm = nimsdata.nimsdicom.NIMSDicom(acq_paths[0])
                    acq_info_dict[acq_no] = (dcm, '%s_%s' % (self.name_prefix, acq_no), '%s a%s' % (self.log_info, acq_no))
                    os.mkdir(arcdir_path)
                    for filepath in acq_paths:
                        os.rename(filepath, '%s.dcm' % os.path.join(arcdir_path, os.path.basename(filepath)))
                    with tarfile.open(arcdir_path+'.tgz', 'w:gz', compresslevel=6) as archive:
                        archive.add(arcdir_path, arcname='%s_%s_dicoms' % (self.name_prefix, acq_no))
                    shutil.rmtree(arcdir_path)
                return acq_info_dict


class PFileReaper(Reaper):

    def __init__(self, url, data_path, pat_id, discard_ids, peripheral_data, sleep_time, tempdir, anonymize):
        self.data_glob = os.path.join(data_path, 'P?????.7')
        id_ = data_path.strip('/').replace('/', '_')
        super(PFileReaper, self).__init__(id_, url, pat_id, discard_ids, peripheral_data, sleep_time, tempdir, anonymize)

    def run(self):
        current_file_datetime = self.reference_datetime
        monitored_files = {}
        while self.alive:
            try:
                reap_files = [self.ReapPFile(self, p) for p in glob.glob(self.data_glob)]
                if not reap_files:
                    raise Warning('No matching files found (or error while checking for files)')
            except (OSError, Warning) as e:
                log.warning(e)
            else:
                reap_files = sorted(filter(lambda f: f.mod_time >= current_file_datetime, reap_files), key=lambda f: f.mod_time)
                for rf in reap_files:
                    rf.parse_pfile()
                    if rf.path in monitored_files:
                        mf = monitored_files[rf.path]
                        if mf.needs_reaping and rf.size == mf.size:
                            rf.reap()
                            if not rf.needs_reaping:
                                self.reference_datetime = current_file_datetime = rf.mod_time
                        elif mf.needs_reaping:
                            log.info('Monitoring  %s' % rf)
                        elif rf.size == mf.size:
                            rf.needs_reaping = False
                    elif rf.pfile is None:
                        rf.needs_reaping = False
                        log.warning('Skipping    %s (unparsable)' % rf.basename)
                    elif rf.pfile.patient_id.strip('/').lower() in self.discard_ids:
                        rf.needs_reaping = False
                        log.info('Discarding  %s' % rf)
                    elif self.pat_id and not re.match(self.pat_id.replace('*','.*'), rf.pfile.patient_id):
                        rf.needs_reaping = False
                        log.info('Ignoring    %s' % rf)
                    else:
                        log.info('Discovered  %s' % rf)
                monitored_files = dict(zip([rf.path for rf in reap_files], reap_files))
            finally:
                if len(monitored_files) < 2:
                    time.sleep(self.sleep_time)


    class ReapPFile(object):

        def __init__(self, reaper, path):
            self.reaper = reaper
            self.path = path
            self.basename = os.path.basename(path)
            self.mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(path))
            self.size = hrsize(os.path.getsize(path))
            self.needs_reaping = True

        def __str__(self):
            return '%s [%s] e%s s%s a%s %s [%s]' % (self.basename, self.size, self.pfile.exam_no, self.pfile.series_no, self.pfile.acq_no, self.mod_time.strftime(DATE_FORMAT), self.pfile.patient_id)

        def parse_pfile(self):
            try:
                self.pfile = nimsdata.nimsraw.NIMSPFile(self.path)
            except nimsdata.nimsraw.NIMSPFileError:
                self.pfile = None
            else:
                self.name_prefix = '%s_%s_%s' % (self.pfile.exam_no, self.pfile.series_no, self.pfile.acq_no)

        def is_auxfile(self, filepath):
            if open(filepath).read(32) == self.pfile._hdr.series.series_uid:
                return True
            try:
                return (nimsdata.nimsraw.NIMSPFile(filepath)._hdr.series.series_uid == self.pfile._hdr.series.series_uid)
            except nimsdata.nimsraw.NIMSPFileError:
                return False

        def reap(self):
            with tempfile.TemporaryDirectory(dir=self.reaper.tempdir) as tempdir_path:
                reap_path = '%s/%s_pfile' % (tempdir_path, self.name_prefix)
                os.mkdir(reap_path)
                auxpaths = [auxpath for auxpath in glob.glob(self.path + '_*') if self.is_auxfile(auxpath)]
                log.debug('Staging     %s' % ', '.join([os.path.basename(path) for path in [self.path] + auxpaths]))
                os.symlink(self.path, os.path.join(reap_path, self.basename))
                for auxpath in auxpaths:
                    os.symlink(auxpath, os.path.join(reap_path, os.path.basename(auxpath)))
                try:
                    log.info('Reaping.tgz %s [%s%s]' % (self.basename, self.size, ' + aux files' if auxpaths else ''))
                    with tarfile.open(reap_path+'.tgz', 'w:gz', dereference=True, compresslevel=4) as archive:
                        archive.add(reap_path, arcname=os.path.basename(reap_path))
                    shutil.rmtree(reap_path)
                except (IOError):
                    log.warning('Error while reaping %s%s' % (self.basename, ' or aux files' if auxpaths else ''))
                else:
                    self.reaper.retrieve_peripheral_data(tempdir_path, self.pfile, self.name_prefix, self.basename)
                    if self.reaper.upload(tempdir_path, self.basename):
                        self.needs_reaping = False
                        log.info('Done        %s' % self.basename)


if __name__ == '__main__':
    import sys
    import signal
    import argparse
    import ConfigParser

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('config_file', help='path to config file')
    arg_parser.add_argument('cls', metavar='class', help='Reaper subclass to use')
    arg_parser.add_argument('class_args', help='subclass arguments')
    arg_parser.add_argument('-a', '--anonymize', action='store_true', help='anonymize patient name and birthdate')
    arg_parser.add_argument('-d', '--discard', default='discard', help='space-separated list of Patient IDs to discard')
    arg_parser.add_argument('-i', '--patid', help='glob for Patient IDs to reap (default: "*")')
    arg_parser.add_argument('-p', '--peripheral', nargs=2, action='append', default=[], help='path to peripheral data')
    arg_parser.add_argument('-s', '--sleeptime', type=int, default=30, help='time to sleep before checking for new data')
    arg_parser.add_argument('-t', '--tempdir', help='directory to use for temporary files')
    arg_parser.add_argument('-u', '--upload_url', help='upload URL')
    args = arg_parser.parse_args()

    config = ConfigParser.ConfigParser({'here': os.path.dirname(os.path.abspath(args.config_file))})
    config.read(args.config_file)
    logging.config.fileConfig(args.config_file, disable_existing_loggers=False)

    try:
        reaper_cls = getattr(sys.modules[__name__], args.cls)
    except AttributeError:
        log.error(args.cls + ' is not a valid Reaper class')
        sys.exit(1)

    url = args.upload_url or config.get('nims', 'api_uri')
    reaper = reaper_cls(url, args.class_args, args.patid, args.discard.split(), dict(args.peripheral), args.sleeptime, args.tempdir, args.anonymize)

    def term_handler(signum, stack):
        reaper.halt()
        log.warning('Received SIGTERM - shutting down...')
    signal.signal(signal.SIGTERM, term_handler)

    reaper.run()
    log.warning('Process halted')
