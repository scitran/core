#!/usr/bin/env python
#
# @author:  Gunnar Schaefer

import os
import time
import threading
import collections

import logging
import logging.config
log = logging.getLogger('scitran.processor')

import tempdir as tempfile


class Processor(object):

    def __init__(self, db, nims_path, worker_provision, tempdir):
        super(Processor, self).__init__()
        self.db = db
        self.worker_provision = worker_provision
        self.nims_path = nims_path
        self.tempdir = tempdir
        self.halted = False

    def halt(self):
        self.halted = True

    def run(self):
        while not self.halted:
            worker_counts = collections.Counter(t.__class__ for t in threading.enumerate())
            log.debug(', '.join('%s: %d' % (w_class.__name__, w_cnt) for w_class, w_cnt in worker_counts.iteritems()))
            for w_class, w_cnt in self.worker_provision.iteritems():
                for i in range(w_cnt - worker_counts[w_class]):
                    w_class(threading.current_thread()).start()
            time.sleep(10)


class Worker(threading.Thread):

    def __init__(self, processor):
        super(Worker, self).__init__()
        self.processor = processor

    def run(self):
        log.debug('launching new %s (%s)' % (self.__class__.__name__, self.name))
        while self.processor.is_alive() and int(time.time()) % 19 != 0:
            time.sleep(5 + threading.current_thread().ident % 5)
        log.debug('exiting       %s (%s)' % (self.__class__.__name__, self.name))


class EpochWorker(Worker):
    pass


class SessionWorker(Worker):
    pass


class ExperimentWorker(Worker):
    pass


class CollectionWorker(Worker):
    pass


if __name__ == '__main__':
    import signal
    import pymongo
    import argparse
    import ConfigParser

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('config_file', help='path to config file')
    arg_parser.add_argument('-we', '--epoch_workers', type=int, help='number of Epoch workers')
    arg_parser.add_argument('-ws', '--session_workers', type=int, help='number of Session workers')
    arg_parser.add_argument('-wx', '--experiment_workers', type=int, help='number of Experiment workers')
    arg_parser.add_argument('-wc', '--collection_workers', type=int, help='number of Collection workers')
    arg_parser.add_argument('-t', '--tempdir', help='directory to use for temporary files')
    arg_parser.add_argument(      '--db_uri', help='NIMS DB URI')
    arg_parser.add_argument(      '--nims_path', help='data destination')
    args = arg_parser.parse_args()

    config = ConfigParser.ConfigParser({'here': os.path.dirname(os.path.abspath(args.config_file))})
    config.read(args.config_file)
    logging.config.fileConfig(args.config_file, disable_existing_loggers=False)

    worker_provision = {}
    worker_provision[EpochWorker] = args.epoch_workers or config.getint('nims', 'epoch_workers')
    worker_provision[SessionWorker] = args.session_workers or config.getint('nims', 'session_workers')
    worker_provision[ExperimentWorker] = args.experiment_workers or config.getint('nims', 'experiment_workers')
    worker_provision[CollectionWorker] = args.collection_workers or config.getint('nims', 'collection_workers')

    db_uri = args.db_uri or config.get('nims', 'db_uri')
    db_client = pymongo.MongoReplicaSetClient(db_uri) if 'replicaSet' in db_uri else pymongo.MongoClient(db_uri)

    nims_path = args.nims_path or config.get('nims', 'nims_path')

    processor = Processor(db_client.get_default_database(), nims_path, worker_provision, args.tempdir)

    def term_handler(signum, stack):
        processor.halt()
        log.info('Receieved SIGTERM - shutting down...')
    signal.signal(signal.SIGTERM, term_handler)

    processor.run()
    log.warning('Process halted')
