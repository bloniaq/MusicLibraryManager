import discogs_client
import logging
import itertools
import time
import string

import config


mod_log = logging.getLogger('main.dgs_con')

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

punctuationremover = str.maketrans('', '', string.punctuation)

ratelimit = config.ratelimit


def find_album_d_master(cat_attrs, f_attrs_list):
    '''tries to return master or release id from discogs db based on inputs'''
    mod_log.info('Finding Album DiscogsID started')
    mod_log.info('Working on {0}'.format(cat_attrs['path']))
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    current_method = ''
    cat_attrs['metoda_did'] = current_method
    cat_attrs['d_master'] = ''
    cat_attrs['d_release'] = ''
    # ile wyników z zapytania brać pod uwagę
    res_tresh = 50
    if cat_attrs['artist'] == 'Unknown Artist' and cat_attrs['album'] == 'Unknown Album':
        mod_log.info('ID not found - Not enough data')
        return cat_attrs
    while True:
        mod_log.info('metoda Album')
        mod_log.info('Connecting Discogs\nQuery: {0}'.format(
            cat_attrs['album']))
        master_album = d.search(cat_attrs['album'], type='master')
        for i in itertools.islice(master_album, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            mod_log.info('Comparing {0} - {1}'.format(martist, malbum))
            if martist == cat_attrs['artist'] and malbum == cat_attrs['album']:
                mod_log.info('Znaleziono ID z listy Album')
                mod_log.info('{0}'.format(i.id))
                current_method = 'Album'
                cat_attrs['d_master'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        mod_log.info('metoda Token')
        mod_log.info('Connecting Discogs\n Query: {0}'.format(token))
        master_token = d.search(token, type='master')
        for i in itertools.islice(master_token, 0, res_tresh):
            mod_log.info('{0}'.format(i.title))
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            mod_log.info('Comparing {0} - {1}'.format(martist, malbum))
            if (martist == cat_attrs['artist'] and
                    malbum == cat_attrs['album']):
                mod_log.info('Znaleziono ID z listy Token')
                mod_log.info('{0}'.format(i.id))
                current_method = 'Token'
                cat_attrs['d_master'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        mod_log.info('metoda Variations')
        mod_log.info('Connecting Discogs\nQuery: {0}'.format(
            cat_attrs['artist']))
        varartist = d.search(cat_attrs['artist'], type='artist')
        variations = varartist[0].name_variations
        mod_log.info('{0}  aliases : {1}'.format(varartist[0], variations))
        if variations is not None:
            for k in variations:
                if cat_attrs['d_master'] != '':
                    break
                tokenvar = k + ' - ' + cat_attrs['album']
                mod_log.info('Connecting Discogs\nQuery: {0}'.format(
                    tokenvar))
                master_variations = d.search(tokenvar, type='master')
                mod_log.info('tried: {0}'.format(tokenvar))
                for i in itertools.islice(master_variations, 0, res_tresh):
                    masterlist = i.title.split(' - ')
                    martist = masterlist[0].split('*')[0]
                    martist = martist.split(' (')[0]
                    malbum = masterlist[1]
                    mod_log.info('Comparing {0} - {1}'.format(
                        martist, malbum))
                    if martist == k and malbum == cat_attrs['album']:
                        mod_log.info('Znaleziono ID z listy Variations')
                        mod_log.info('{0}'.format(i.id))
                        cat_attrs['d_master'] = i.id
                        current_method = 'Variations'
                        break
                time.sleep(ratelimit)
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        i = 0
        mod_log.info('metoda Release')
        mod_log.info(
            'Connecting Discogs\nQuery: {0}'.format(cat_attrs['album']))
        release = d.search(cat_attrs['album'], type='release')
        for i in itertools.islice(release, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            mod_log.info('Comparing {0} - {1}'.format(martist, malbum))
            if (martist == cat_attrs['artist'] and
                    malbum == cat_attrs['album']):
                mod_log.info('Znaleziono ID z listy Release')
                mod_log.info('{0}'.format(i.id))
                current_method = 'Release'
                cat_attrs['d_release'] = i.id
                break
        if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
            break
        time.sleep(ratelimit)
        mod_log.info('metoda Release2')
        token_improved = token
        mod_log.info('Connecting Discogs\nQuery: {0}'.format(
            token_improved))
        release2 = d.search(token_improved, type='release')
        for i in itertools.islice(release2, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0].translate(punctuationremover)
            malbum = masterlist[1].translate(punctuationremover)
            tranartist = cat_attrs['artist'].translate(punctuationremover)
            tranalbum = cat_attrs['album'].translate(punctuationremover)
            mod_log.info('Comparing {0} - {1}'.format(martist, malbum))
            mod_log.info('With {0} - {1}'.format(tranartist, tranalbum))
            if martist == tranartist and malbum == tranalbum:
                mod_log.info('Znaleziono ID z listy Release2')
                mod_log.info('{0}'.format(i.id))
                current_method = 'Release2'
                cat_attrs['d_release'] = i.id
                break
        time.sleep(ratelimit)
        break
    if cat_attrs['d_master'] != '' and cat_attrs['d_release']:
        mod_log.info('Found ID by {0}'.format(cat_attrs['metoda_did']))
        mod_log.info('The founded ID is {0}'.format(cat_attrs['d_master']))
    else:
        mod_log.info('ID not found')
    cat_attrs['metoda_did'] = current_method

    return cat_attrs
