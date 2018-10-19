import discogs_client
import logging
import itertools
import time
import string

import config
import text_tools


log = logging.getLogger('main.dgs_con')

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

punctuationremover = str.maketrans('', '', string.punctuation)

ratelimit = config.ratelimit
checklist = config.discogs_checklist


def is_found(dictonary, key):
    if key in dictonary:
        return True
    else:
        return False


def insert_ids(cat_attrs, f_attrs_list):
    log.info('Query Discogs Func started')
    log.info('Working on {0}'.format(cat_attrs['path']))
    if not is_found(cat_attrs, 'd_master'):
        cat_attrs = find_a_master(cat_attrs, f_attrs_list)
    if not is_found(cat_attrs, 'd_release'):
        cat_attrs = find_a_release(cat_attrs, f_attrs_list)
    for i in checklist:
        cat_attrs[i] = cat_attrs.get(i, '')
    log.info(
        'Finshed querying Discogs, results are: {0}, {1}, {2}, {3}\n\n'.format(
            cat_attrs['d_master'], cat_attrs['metoda_master'],
            cat_attrs['d_release'], cat_attrs['metoda_release']))
    return cat_attrs


##########################################
# FINDING MASTER
##########################################


def find_a_master(cat_attrs, f_attrs_list):
    log.info('Find a master Func started')
    while True:
        m_by_album(cat_attrs, 50)
        if is_found(cat_attrs, 'd_master'):
            break
        m_by_token(cat_attrs, 15)
        if is_found(cat_attrs, 'd_master'):
            break
        m_by_variations(cat_attrs, 100)
        if is_found(cat_attrs, 'd_master'):
            break
        break
    log.info('Find a master Func ended\n')
    return cat_attrs


def m_by_album(cat_attrs, res_tresh):
    log.info('Master: Album')
    log.info('Connecting Discogs\tQuery: {0}'.format(
        cat_attrs['album']))
    cur_method = 'Album'
    outcome = d.search(cat_attrs['album'], type='master')
    for i in itertools.islice(outcome, 0, res_tresh):
        m_name = i.title.split(' - ')
        m_artist = m_name[0]
        m_album = m_name[1]
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, cat_attrs['artist'], cat_attrs['album']))
        if m_artist == cat_attrs['artist'] and m_album == cat_attrs['album']:
            cat_attrs['metoda_master'] = cur_method
            cat_attrs['d_master'] = i.id
            log.info('Found ID : {0} by a {1} method\n'.format(
                i.id, cat_attrs['metoda_master']))
            break
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_master'], cat_attrs['d_master']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs


def m_by_token(cat_attrs, res_tresh):
    log.info('Master: Token')
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    log.info('Connecting Discogs\tQuery: {0}'.format(token))
    cur_method = 'Token'
    outcome = d.search(token, type='master')
    for i in itertools.islice(outcome, 0, res_tresh):
        log.info('{0}'.format(i.title))
        m_name = i.title.split(' - ')
        m_artist = m_name[0]
        m_album = m_name[1]
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, cat_attrs['artist'], cat_attrs['album']))
        if m_artist == cat_attrs['artist'] and m_album == cat_attrs['album']:
            cat_attrs['metoda_master'] = cur_method
            cat_attrs['d_master'] = i.id
            log.info('Found ID : {0} by a {1} method\n'.format(
                i.id, cat_attrs['metoda_master']))
            break
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_master'], cat_attrs['d_master']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs


def m_by_variations(cat_attrs, res_tresh):
    log.info('Master: Variations')
    log.info('Connecting Discogs\tQuery: {0}'.format(
        cat_attrs['artist']))
    cur_method = 'Variations'
    outcome = d.search(cat_attrs['artist'], type='artist')
    try:
        variations = outcome[0].name_variations
    except IndexError as e:
        variations = None
        log.warning('No variations of {0} found'.format(cat_attrs['artist']))
    else:
        if variations is not None:
            log.info('Found {0} aliases : {1}'.format(outcome[0], variations))
            for k in variations:
                if is_found(cat_attrs, 'd_master'):
                    break
                variations_query = k + ' - ' + cat_attrs['album']
                log.info('Connecting Discogs\tQuery: {0}'.format(
                    variations_query))
                outcome = d.search(variations_query, type='master')
                for i in itertools.islice(outcome, 0, res_tresh):
                    m_name = i.title.split(' - ')
                    m_artist = m_name[0]
                    m_album = m_name[1]
                    # m_artist = masterlist[0].split('*')[0]
                    # m_artist = martist.split(' (')[0]
                    log.info('Comparing {0} - {1} to {2} - {3}'.format(
                        m_artist, m_album, cat_attrs['artist'], cat_attrs['album']))
                    if (m_artist == cat_attrs['artist'] and
                            m_album == cat_attrs['album']):
                        cat_attrs['metoda_master'] = cur_method
                        cat_attrs['d_master'] = i.id
                        log.info('Found ID : {0} by a {1} method\n'.format(
                            i.id, cat_attrs['metoda_master']))
                        break
                time.sleep(ratelimit)
        else:
            log.warning('No variations of {0} found - Check LOG'.format(outcome[0]))
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_master'], cat_attrs['d_master']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs


##########################################
# FINDING RELEASE
##########################################


def find_a_release(cat_attrs, f_attrs_list):
    log.info('Find a master Func started')
    while True:
        r_by_album(cat_attrs, 15)
        if is_found(cat_attrs, 'd_release'):
            break
        r_by_token(cat_attrs, 15)
        if is_found(cat_attrs, 'd_release'):
            break
        break
    log.info('Find a release Func started\n')
    return cat_attrs


def r_by_album(cat_attrs, res_tresh):
    log.info('Release: Album')
    log.info('Connecting Discogs\tQuery: {0}'.format(cat_attrs['album']))
    cur_method = 'Album'
    outcome = d.search(cat_attrs['album'], type='release')
    for i in itertools.islice(outcome, 0, res_tresh):
        if len(i.artists) > 1:
            log.debug('there is more than one artist in current release')
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            i.artists[0], i.title, cat_attrs['artist'], cat_attrs['album']))
        if (i.artists[0] == cat_attrs['artist'] and
                i.title == cat_attrs['album']):
            cat_attrs['metoda_release'] = cur_method
            cat_attrs['d_release'] = i.id
            log.info('Found ID : {0} by a {1} method\n'.format(
                i.id, cat_attrs['metoda_release']))
            break
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_release'], cat_attrs['d_release']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs

def r_by_token(cat_attrs, res_tresh):
    log.info('Release: Token')
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    log.info('Connecting Discogs\tQuery: {0}'.format(token))
    cur_method = 'Token'
    outcome = d.search(token, type='release')
    for i in itertools.islice(outcome, 0, res_tresh):
        m_artist = str(i.artists[0]).translate(punctuationremover)
        m_album = str(i.title).translate(punctuationremover)
        tran_artist = cat_attrs['artist'].translate(punctuationremover)
        tran_album = cat_attrs['album'].translate(punctuationremover)
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, tran_artist, tran_album))
        if m_artist == tran_artist and m_album == tran_album:
            cat_attrs['metoda_release'] = cur_method
            cat_attrs['d_release'] = i.id
            log.info('Found ID : {0} by a {1} method\n'.format(
                i.id, cat_attrs['metoda_release']))
            break
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_release'], cat_attrs['d_release']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs
