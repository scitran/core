#!/usr/bin/env python

import copy
import dateutil.parser
import dicom
import elasticsearch
import json
import logging

from api import config, encoder

es = config.es
db = config.db

DICOM_INDEX = 'dicom_store'

def datetime(str_datetime):
    pass

def age(str_age):
    pass


SKIPPED = ['PixelSpacing', 'ImageOrientationPatient', 'PatientAge', 'ImagePositionPatient']

# TODO: Choose integer/long and float/double where appropriate
VR_TYPES = {
        'AE': ['string', str],
        'AS': ['long', int],
        'AT': ['long', int],
        'CS': ['string', str],
        'DA': ['date', datetime],
        'DS': ['float', float],
        'DT': ['date', datetime],
        'FD': ['float', float],
        'FL': ['float', float],
        'IS': ['long', int],
        'LO': ['string', str],
        'LT': ['string', str],
        'NONE': None,
        'OB': None,
        'OB or OW': None,
        'OF': None,
        'OW': None,
        'PN': ['string', str],
        'SH': ['string', str],
        'SL': ['long', int],
        'SQ': None,
        'SS': ['long', int],
        'ST': ['string', str],
        'TM': ['date', datetime],
        'UI': ['string', str],
        'UL': ['long', int],
        'US': ['long', int],
        'US or OW': ['long', int],
        'US or SS': ['long', int],
        'US or SS or OW': ['long', int],
        'UT': ['string', str]
    }

def create_mappings():
    public_dict = dicom._dicom_dict.DicomDictionary
    field_mappings = {}
    for k,v in public_dict.iteritems():
        vr_type = v[0]
        field_name = v[4]
        vr_mapping = VR_TYPES.get(vr_type)
        if vr_mapping:
            field_mappings[field_name] = {'type': vr_mapping[0]}
        else:
            pass
            #logging.warn('Skipping field {} of VR type {}'.format(field_name, vr_type))

    return field_mappings


if __name__ == '__main__':

    if es.indices.exists(DICOM_INDEX):
        print 'Removing existing dicom_store index...'
        res = es.indices.delete(index=DICOM_INDEX)
        print 'response: {}'.format(res)

    mappings = create_mappings()

    request = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mappings': {
            'dicom': {
                'properties': {
                    'dicom_header': {
                        'properties': mappings
                    }
                }
            }
        }
    }

    print 'creating {} index ...'.format(DICOM_INDEX)
    res = es.indices.create(index=DICOM_INDEX, body=request)
    print 'response: {}'.format(res)

    groups = db.groups.find({})
    for g in groups:
        g.pop('roles', None)
        projects = db.projects.find({'group': g['_id']})
        for p in projects:
            p.pop('permissions', None)
            sessions = db.sessions.find({'project': p['_id']})
            for s in sessions:
                s.pop('permissions', None)
                acquisitions = db.acquisitions.find({'session': s['_id'], 'files.type': 'dicom'})
                for a in acquisitions:

                    dicom_data = a.get('metadata')
                    if dicom_data:
                        for s in SKIPPED:
                            dicom_data.pop(s, None)

                        permissions = a['permissions']

                        doc = {
                            'dicom_header':         dicom_data,
                            'base_container_type': 'acquisition',
                            'acquisition':          a,
                            'session':              s,
                            'project':              p,
                            'group':                g,
                            'permissions':          a['permissions']

                        }

                        doc = json.dumps(doc, default=encoder.custom_json_serializer)
                        es.index(index=DICOM_INDEX, doc_type='dicom', body=doc)

