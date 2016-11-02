#!/usr/bin/env python

import copy
import dateutil.parser
import dicom
import elasticsearch
import json
import logging

from api import config

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
                'properties': mappings
            }
        }
    }

    print 'creating {} index ...'.format(DICOM_INDEX)
    res = es.indices.create(index=DICOM_INDEX, body=request)
    print 'response: {}'.format(res)

    acquisitions = db.acquisitions.find({'files.type': 'dicom'})
    for a in acquisitions:
        dicom_data = a.get('metadata')
        if dicom_data:
            for s in SKIPPED:
                dicom_data.pop(s, None)
            es.index(index=DICOM_INDEX, doc_type='dicom', body=dicom_data)

