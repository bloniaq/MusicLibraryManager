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


def is_found(key):
    if key in cat_attrs:
        return True
    else:
        return False


def query_discogs(cat_attrs, f_attrs_list):
    log.info('Query Discogs Func started')
    log.info('Working on {0}'.format(cat_attrs['path']))
    cat_attrs = find_a_master(cat_attrs, f_attrs_list)
    cat_attrs = find_a_release(cat_attrs, f_attrs_list)
    for i in checklist:
        cat_attrs.get(i, '')
    return cat_attrs


##########################################
# FINDING MASTER
##########################################


def find_a_master(cat_attrs, f_attrs_list):
    while True:
        m_by_album(cat_attrs, 50)
        if is_found('d_master'):
            break
        m_by_token(cat_attrs, 15)
        if is_found('d_master'):
            break
        m_by_variations(cat_attrs, 100)
        if is_found('d_master'):
            break
        break
    return cat_attrs


def m_by_album(cat_attrs, res_tresh):
    log.info('Master: Album')
    log.info('Connecting Discogs\t\tQuery: {0}'.format(
        cat_attrs['album']))
    cur_method = 'Album'
    outcome = d.search(cat_attrs['album'], type='master')
    for i in itertools.islice(outcome, 0, res_tresh):
        m_name = i.title.split(' - ')
        m_artist = masterlist[0]
        m_album = masterlist[1]
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, cat_attrs['artist'], cat_attrs['album']))
        if m_artist == cat_attrs['artist'] and m_album == cat_attrs['album']:
            cat_attrs['metoda_master'] = cur_method
            cat_attrs['d_master'] = i.id
            log.info('Found ID : {0} by a {1} method'.format(
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
    log.info('Connecting Discogs\t\tQuery: {0}'.format(token))
    cur_method = 'Token'
    outcome = d.search(token, type='master')
    for i in itertools.islice(outcome, 0, res_tresh):
        log.info('{0}'.format(i.title))
        m_name = i.title.split(' - ')
        m_artist = masterlist[0]
        m_album = masterlist[1]
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, cat_attrs['artist'], cat_attrs['album']))
        if m_artist == cat_attrs['artist'] and m_album == cat_attrs['album']:
            cat_attrs['metoda_master'] = cur_method
            cat_attrs['d_master'] = i.id
            log.info('Found ID : {0} by a {1} method'.format(
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
    log.info('Connecting Discogs\t\tQuery: {0}'.format(
        cat_attrs['artist']))
    cur_method = 'Variations'
    outcome = d.search(cat_attrs['artist'], type='artist')
    variations = outcome[0].name_variations
    if variations is not None:
        log.info('Found {0} aliases : {1}'.format(outcome[0], variations))
        for k in variations:
            if is_found('d_master'):
                break
            variations_query = k + ' - ' + cat_attrs['album']
            log.info('Connecting Discogs\nQuery: {0}'.format(
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
                    log.info('Found ID : {0} by a {1} method'.format(
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
    while True:
        r_by_album(cat_attrs, 15)
        if is_found('d_release'):
            break
        r_by_token(cat_attrs, 15)
        if is_found('d_release'):
            break
        break
    return cat_attrs


def r_by_album(cat_attrs, res_tresh):
    log.info('Release: Album')
    log.info('Connecting Discogs\nQuery: {0}'.format(cat_attrs['album']))
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
            log.info('Found ID : {0} by a {1} method'.format(
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
    log.info('Master: Token')
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    log.info('Connecting Discogs\t\tQuery: {0}'.format(token))
    cur_method = 'Token'
    outcome = d.search(token, type='release')
    for i in itertools.islice(outcome, 0, res_tresh):
        m_artist = i.artists[0].translate(punctuationremover)
        m_album = i.title.translate(punctuationremover)
        tran_artist = cat_attrs['artist'].translate(punctuationremover)
        tran_album = cat_attrs['album'].translate(punctuationremover)
        log.info('Comparing {0} - {1} to {2} - {3}'.format(
            m_artist, m_album, tran_artist, tran_album))
        if m_artist == tran_artist and m_album == tran_album:
            cat_attrs['metoda_release'] = cur_method
            cat_attrs['d_release'] = i.id
            log.info('Found ID : {0} by a {1} method'.format(
                i.id, cat_attrs['metoda_release']))
            break
    try:
        log.debug('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_release'], cat_attrs['d_release']))
    except BaseException as e:
        log.debug('no values on output, error: {0}\n'.format(e))
    time.sleep(ratelimit)
    return cat_attrs

"""
def find_album_d_master(cat_attrs, f_attrs_list):
    '''tries to return master or release id from discogs db based on inputs'''
    log.info('Finding Album DiscogsID started')
    log.info('Working on {0}'.format(cat_attrs['path']))
    
    current_method = ''
    cat_attrs['metoda_did'] = current_method
    cat_attrs['d_master'] = ''
    cat_attrs['d_release'] = ''
    # ile wyników z zapytania brać pod uwagę
    res_tresh = 50
    while True:
        log.info('metoda Album')
        log.info('Connecting Discogs\nQuery: {0}'.format(
            cat_attrs['album']))
        master_album = d.search(cat_attrs['album'], type='master')
        for i in itertools.islice(master_album, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            if martist == cat_attrs['artist'] and malbum == cat_attrs['album']:
                log.info('Znaleziono ID z listy Album')
                log.info('{0}'.format(i.id))
                current_method = 'Album'
                cat_attrs['d_master'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        log.info('metoda Token')
        log.info('Connecting Discogs\n Query: {0}'.format(token))
        master_token = d.search(token, type='master')
        for i in itertools.islice(master_token, 0, res_tresh):
            log.info('{0}'.format(i.title))
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            if (martist == cat_attrs['artist'] and
                    malbum == cat_attrs['album']):
                log.info('Znaleziono ID z listy Token')
                log.info('{0}'.format(i.id))
                current_method = 'Token'
                cat_attrs['d_master'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        log.info('metoda Variations')
        log.info('Connecting Discogs\nQuery: {0}'.format(
            cat_attrs['artist']))
        varartist = d.search(cat_attrs['artist'], type='artist')
        variations = varartist[0].name_variations
        log.info('{0}  aliases : {1}'.format(varartist[0], variations))
        if variations is not None:
            for k in variations:
                if cat_attrs['d_master'] != '':
                    break
                tokenvar = k + ' - ' + cat_attrs['album']
                log.info('Connecting Discogs\nQuery: {0}'.format(
                    tokenvar))
                master_variations = d.search(tokenvar, type='master')
                log.info('tried: {0}'.format(tokenvar))
                for i in itertools.islice(master_variations, 0, res_tresh):
                    masterlist = i.title.split(' - ')
                    martist = masterlist[0].split('*')[0]
                    martist = martist.split(' (')[0]
                    malbum = masterlist[1]
                    log.info('Comparing {0} - {1}'.format(
                        martist, malbum))
                    if martist == k and malbum == cat_attrs['album']:
                        log.info('Znaleziono ID z listy Variations')
                        log.info('{0}'.format(i.id))
                        cat_attrs['d_master'] = i.id
                        current_method = 'Variations'
                        break
                time.sleep(ratelimit)
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        log.info('metoda Release')
        log.info(
            'Connecting Discogs\nQuery: {0}'.format(cat_attrs['album']))
        release = d.search(cat_attrs['album'], type='release')
        for i in itertools.islice(release, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            if (martist == cat_attrs['artist'] and
                    malbum == cat_attrs['album']):
                log.info('Znaleziono ID z listy Release')
                log.info('{0}'.format(i.id))
                current_method = 'Release'
                cat_attrs['d_release'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        log.info('metoda Release2')
        token_improved = token
        log.info('Connecting Discogs\nQuery: {0}'.format(
            token_improved))
        release2 = d.search(token_improved, type='release')
        for i in itertools.islice(release2, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0].translate(punctuationremover)
            malbum = masterlist[1].translate(punctuationremover)
            tranartist = cat_attrs['artist'].translate(punctuationremover)
            tranalbum = cat_attrs['album'].translate(punctuationremover)
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            log.info('With {0} - {1}'.format(tranartist, tranalbum))
            if martist == tranartist and malbum == tranalbum:
                log.info('Znaleziono ID z listy Release2')
                log.info('{0}'.format(i.id))
                current_method = 'Release2'
                cat_attrs['d_release'] = i.id
                break
        time.sleep(ratelimit)
        break
    if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
        log.info('Found ID by {0}'.format(cat_attrs['metoda_did']))
        log.info('The founded ID is {0}'.format(cat_attrs['d_master']))
    else:
        log.info('ID not found')
    cat_attrs['metoda_did'] = current_method

    return cat_attrs
"""