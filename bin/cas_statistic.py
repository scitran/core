#!/usr/bin/env python

import os
import pymongo
from collections import Counter

db_uri = os.getenv('SCITRAN_PERSISTENT_DB_URI', 'localhost:9001')
db = pymongo.MongoClient(db_uri).get_database('scitran')

COLLECTIONS = ['projects', 'acquisitions', 'analyses']
COLLECTIONS_WITH_EMBEDDED = [('sessions', 'subject')]


def files_of_collection(collection, embedded_doc=None):
    hash_size_pairs = []
    cursor = db.get_collection(collection).find({})
    for document in cursor:
        hash_size_pairs += files_of_document(document)
        if embedded_doc:
            hash_size_pairs += files_of_document(document.get(embedded_doc, {}))

    return hash_size_pairs


def files_of_document(document):
    hash_size_pairs = []
    files = document.get('files', [])
    for f in files:
        hash_size_pairs.append((f['hash'], f['size']))

    return hash_size_pairs


def main():
    hash_size_pairs = []
    for collection in COLLECTIONS:
        hash_size_pairs += files_of_collection(collection)

    for collection, embedded_doc in COLLECTIONS_WITH_EMBEDDED:
        hash_size_pairs += files_of_collection(collection, embedded_doc)

    counter = Counter(hash_size_pairs)
    size_with_cas = 0
    size_wo_cas = 0
    file_count_cas = len(counter)
    file_count_wo_cas = 0

    for hash_size_pair in counter:
        size_with_cas += hash_size_pair[1]
        size_wo_cas += hash_size_pair[1] * counter[hash_size_pair]
        file_count_wo_cas += counter[hash_size_pair]

    saved_disk_space = size_wo_cas - size_with_cas

    print('Total size (CAS): %s Bytes' % size_with_cas)
    print('Total size (wo CAS): %s Bytes' % size_wo_cas)
    print('Number of files (CAS): %s' % file_count_cas)
    print('Number of files (wo CAS): %s' % file_count_wo_cas)
    print('Saved disk space: %s Bytes (%s%%)' % (
        saved_disk_space, round(saved_disk_space / float(size_wo_cas) * 100, 2)))


if __name__ == '__main__':
    main()
