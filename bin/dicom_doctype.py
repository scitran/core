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
            field_type = vr_mapping[0]
            if field_type == 'string' and vr_type not in ['UT', 'LT', 'ST']:
                config.log
                field_mappings[field_name+'_term'] = {'type': 'string', 'index': 'not_analyzed'}
            field_mappings[field_name] = {'type': field_type}
        else:
            pass
            #logging.warn('Skipping field {} of VR type {}'.format(field_name, vr_type))

    return field_mappings

def cast_date(dcm_date):
    """
    Cast DICOM date string (YYYYMMDD) into ElasticSearch pre-defined strict_date format (yyyy-MM-dd)
    """
    return dcm_date[:4] + '-' + dcm_date[4:6] + '-' + dcm_date[6:]

def cast_time(dcm_time):
    """
    Cast DICOM time string (HHMMSS.FRAC)
    into ElasticSearch pre-defined strict_time format (HH:mm:ss.SSSZZ)
    """
    # TODO: this fxn needs to be tested on real data
    if len(dcm_time) < 6:
        return None
    hours = dcm_time[:2]
    minutes = dcm_time[2:4]
    seconds = dcm_time[4:6]
    if len(dcm_time) > 7:
        fraction_str = dcm_time[7:]
        fraction = float(dcm_time[7:])/10^(len(fraction_str))
        fraction = int(fraction*1000)
    else:
        fraction = 0
    return '%s:%s:%s.%03d00' % (hours, minutes, seconds, fraction)

def cast_datetime(dcm_datetime):
    """
    Cast DICOM datetime string (YYYYMMDDHHMMSS.FFFFFF)
    into ElasticSearch pre-defined basic_date_time format (yyyyMMdd'T'HHmmss.SSSZ)
    """
    # TODO: this fxn needs to be tested on real data
    year = dcm_datetime[:4]
    month = dcm_datetime[4:6]
    day = dcm_datetime[6:8]
    if len(dcm_datetime) > 8:
        hours = dcm_datetime[8:10]
        minutes = dcm_datetime[10:12]
        seconds = dcm_datetime[12:14]
    else:
        hours = '00'
        minutes = '00'
        seconds = '00'
    if len(dcm_datetime) > 15:
        fraction_str = dcm_datetime[15:]
        fraction = float(dcm_datetime[15:])/10^(len(fraction_str))
        fraction = int(fraction*1000)
    else:
        fraction = 0
    return '%s%s%sT%s%s%s.%03d0' % (year, month, day, hours, minutes, seconds, fraction)

def cast_age(dcm_age):
    """ Cast DICOM age string into seconds"""
    # TODO: this fxn needs to be tested on real data
    unit = dcm_age[-1]
    if unit not in ['D', 'W', 'M', 'Y']:
        return None
    multipliers = dict(D=60*60*24,
                       W=60*60*24*7,
                       M=60*60*24*30,
                       Y=60*60*24*365)
    value = int(dcm_age[:-1])
    seconds = multipliers[unit]*value
    return seconds



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

    mappings = es.indices.get_mapping(index=DICOM_INDEX, doc_type='dicom')
    dicom_mappings = mappings['dicom_store']['mappings']['dicom']['properties']['dicom_header']['properties']

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
                        term_fields = {}
                        for s in SKIPPED:
                            dicom_data.pop(s, None)
                        for k,v in dicom_data.iteritems():
                            if 'datetime' in k.lower():
                                config.log.debug('called for {}'.format(k))
                                v = cast_datetime(str(v))
                            elif 'date' in k.lower():
                                config.log.debug('called for {}'.format(k))
                                v = cast_date(str(v))
                            elif 'time' in k.lower():
                                config.log.debug('called for {}'.format(k))
                                v = cast_time(str(v))

                            term_field_name = k+'_term'
                            if term_field_name in dicom_mappings:
                                term_fields[k+'_term'] = str(v)
                        dicom_data.update(term_fields)

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

