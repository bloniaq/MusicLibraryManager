import discogs_client
import logging
import config

import discogs_tools
import discogs_meths


log = logging.getLogger('main.dgs_con')

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

ratelimit = config.ratelimit
checklist = config.discogs_checklist

config.signal_trig()


def insert_ids(cat_attrs, f_attrs_list):
    log.info('Query Discogs Func started')
    log.info('Working on {}\n'.format(cat_attrs['path']))
    cat_attrs['comment'] += ''
    if not discogs_tools.is_found(cat_attrs, 'd_master'):
        cat_attrs = find_a_master(cat_attrs, f_attrs_list)
    if config.is_interrupted():
        return cat_attrs
    if not discogs_tools.is_found(cat_attrs, 'd_release', 'd_master'):
        cat_attrs = find_a_release(cat_attrs, f_attrs_list)
    if config.is_interrupted():
        return cat_attrs
    for i in checklist:
        cat_attrs[i] = cat_attrs.get(i, '')
    log.info(
        'Finshed querying Discogs, results are: {}, {}, {}, {}\n\n'.format(
            cat_attrs['d_master'], cat_attrs['metoda_master'],
            cat_attrs['d_release'], cat_attrs['metoda_release']))
    return cat_attrs


##########################################
# FINDING MASTER
##########################################


def find_a_master(cat_attrs, f_attrs_list, skip=False):
    log.info('Find a master Func started\n')
    discogs_meths.m_by_token(cat_attrs, 10)
    if discogs_tools.is_found(cat_attrs, 'd_master'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    discogs_meths.m_by_album(cat_attrs, 50)
    if discogs_tools.is_found(cat_attrs, 'd_master'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    discogs_meths.m_by_variations(cat_attrs, 100)
    if discogs_tools.is_found(cat_attrs, 'd_master'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    discogs_meths.m_by_token_cut(cat_attrs, 10)
    if discogs_tools.is_found(cat_attrs, 'd_master'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    discogs_meths.m_by_album_fuzz(cat_attrs)
    if discogs_tools.is_found(cat_attrs, 'd_master', 'd_release'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    discogs_meths.m_by_album_fuzz_excl(cat_attrs)
    if discogs_tools.is_found(cat_attrs, 'd_master', 'd_release'):
        return cat_attrs
    if config.is_interrupted():
        return cat_attrs
    if config.manual_mode:
        discogs_meths.m_by_manual(cat_attrs)
    if discogs_tools.is_found(cat_attrs, 'd_master'):
        return cat_attrs
    discogs_meths.m_by_artist(cat_attrs)
    if config.is_interrupted():
        return cat_attrs
    log.info('Find a master Func ended\n\n')
    return cat_attrs


def find_a_release(cat_attrs, f_attrs_list):
    log.info('Find a release Func started\n')
    discogs_meths.r_by_token(cat_attrs, 15)
    if discogs_tools.is_found(cat_attrs, 'd_release'):
        return cat_attrs
    discogs_meths.r_by_album(cat_attrs, 15)
    if discogs_tools.is_found(cat_attrs, 'd_release'):
        return cat_attrs
    log.info('Find a release Func started\n\n')
    return cat_attrs
