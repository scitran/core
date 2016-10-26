#!/usr/bin/env python

import copy
import dateutil.parser
import dicom
import elasticsearch
import json
import logging

from api import config

es = config.es

DICOM_INDEX = 'dicom_store'

def datetime(str_datetime):
    pass

def age(str_age):
    pass

VR_TYPES = {
        'AE': str,
        'AS': age,
        'AT': int,
        'CS': str,
        'DA': datetime,
        'DS': float,
        'DT': datetime,
        'FD': float,
        'FL': float,
        'IS': int,
        'LO': str,
        'LT': str,
        'NONE': None,
        'OB': None,
        'OB or OW': None,
        'OF': None,
        'OW': None,
        'PN': str,
        'SH': str,
        'SL': int,
        'SQ': None,
        'SS': int,
        'ST': str,
        'TM': datetime,
        'UI': str,
        'UL': int,
        'US': int,
        'US or OW': int,
        'US or SS': int,
        'US or SS or OW': int,
        'UT': str,
    }

if __name__ == '__main__':

    #public_dict = dicom._dicom_dict.DicomDictionary

    if es.indices.exists(DICOM_INDEX):
        print 'Removing existing dicom_store index...'
        res = es.indices.delete(index=DICOM_INDEX)
        print 'response: {}'.format(res)

    request = {
        'settings': {
            'number_of_shards': 1,
            'number_of_replicas': 0
        },
        'mappings': {
            'properties': {
                'title': {
                    'type': 'string'
                }
            }
        }
    }

    print 'creating {} index ...'.format(DICOM_INDEX)
    res = es.indices.create(index=DICOM_INDEX, body=request)
    print 'response: {}'.format(res)

