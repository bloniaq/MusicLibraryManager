import logging
import discogs_client
import time
import itertools
import requests
import string
from functools import wraps
from datetime import timedelta
from fuzzywuzzy import fuzz
from json import decoder

import config
import text_tools
import discogs_tools

log = logging.getLogger('main.dgs_meths')

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

punctuationremover = str.maketrans('', '', string.punctuation)

ratelimit = config.ratelimit
checklist = config.discogs_checklist

config.signal_trig()


def retry(exceptions, tries=4, delay=3, backoff=2, logger=log):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        if hasattr(e, 'status_code'):
                            if e.status_code == 404:
                                logger.warning('\n\n\n\n\n\n\n\n\n')
                                logger.warning(
                                    'PORAZKA z tym wyjątkiem 404 przeszedł')
                        logger.warning('\n\n\n\n\n\n\n\n\n')
                        logger.warning(msg)
                        logger.warning('\n\n\n\n\n\n\n\n\n')
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
                    if mtries == 1:
                        return f(*args, **kwargs, skip=True)
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_album(cat_attrs, res_tresh, skip=False):
    log.info('Master: Album\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(
        cat_attrs['album']))
    cur_method = 'Album'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    outcome = d.search(cat_attrs['album'], type='master')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    counter = 0
    for i in itertools.islice(outcome, 0, res_tresh):
        counter += 1
        if config.is_interrupted():
            return cat_attrs
        m_name = i.title.split(' - ')
        result = text_tools.rm_artist_num(m_name[0]), m_name[1]
        cat_attrs, flag = discogs_tools.final_comparison(
            cat_attrs, result, i.id, cur_method, counter)
        if flag:
            break
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_token(cat_attrs, res_tresh, skip=False):
    log.info('Master: Token\n')
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    log.info('***Connecting Discogs\tQuery: {0}'.format(token))
    cur_method = 'Token'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    outcome = d.search(token, type='master')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    if outcome is None:
        log.info('Found None querying token\n')
        return cat_attrs
    counter = 0
    for i in itertools.islice(outcome, 0, res_tresh):
        if config.is_interrupted():
            return cat_attrs
        counter += 1
        log.info('{} {}'.format(counter, i.title))
        m_name = i.title.split(' - ')
        result = text_tools.rm_artist_num(m_name[0]), m_name[1]
        cat_attrs, flag = discogs_tools.final_comparison(
            cat_attrs, result, i.id, cur_method, counter)
        if flag:
            break
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_variations(cat_attrs, res_tresh, skip=False):
    log.info('Master: Variations\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(
        cat_attrs['artist']))
    cur_method = 'Variations'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    outcome = d.search(cat_attrs['artist'], type='artist')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    try:
        variations = outcome[0].name_variations
        log.info('Found {0} aliases : {1}'.format(outcome[0], variations))
        for k in variations:
            if config.is_interrupted():
                return cat_attrs
            counter = 0
            variations_query = k + ' - ' + cat_attrs['album']
            log.info('***Connecting Discogs\tQuery: {0}'.format(
                variations_query))
            outcome = d.search(variations_query, type='master')
            log.info('FOUND {0} RESULTS'.format(len(outcome)))
            flag = False
            for i in itertools.islice(outcome, 0, res_tresh):
                counter += 0
                m_name = i.title.split(' - ')
                # m_artist = masterlist[0].split('*')[0]
                # m_artist = martist.split(' (')[0]
                result = m_name[0], m_name[1]
                cat_attrs, flag = discogs_tools.final_comparison(
                    cat_attrs, result, i.id, cur_method, counter)
                if flag:
                    break
            if flag:
                break
            time.sleep(ratelimit)
    except TypeError as e:
        log.warning('{} No variations of {} found'.format(
            e, cat_attrs['artist']))
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_token_cut(cat_attrs, res_tresh, skip=False):
    log.info('Master: Token Cut\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(
        cat_attrs['album']))
    cur_method = 'Token Cuted'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    album = text_tools.fm_album_suffixes(cat_attrs['album'])
    token = cat_attrs['artist'] + ' - ' + album
    outcome = d.search(token, type='master')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    counter = 0
    for i in itertools.islice(outcome, 0, res_tresh):
        if config.is_interrupted():
            return cat_attrs
        counter += 1
        m_name = i.title.split(' - ')
        result = text_tools.rm_artist_num(m_name[0]), m_name[1]
        cat_attrs, flag = discogs_tools.final_comparison(
            cat_attrs, result, i.id, cur_method, counter)
        if flag:
            break
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs


def m_by_manual(cat_attrs):
    cur_method = 'Manual'
    total_length = str(timedelta(seconds=cat_attrs['total_length']))
    log.info('Master: Manual\n')
    log.info('path : {0}'.format(cat_attrs['path']))
    log.info('artist : {0}'.format(cat_attrs['artist']))
    log.info('album : {0}'.format(cat_attrs['album']))
    log.info('date : {0}'.format(cat_attrs['date']))
    log.info('length : {0}'.format(total_length))
    log.info('tracks : {0}'.format(cat_attrs['total_tracks']))
    decision = input(
        'Do you find id of it on discogs? [Master/Release/None]: ')
    if decision == 'm' or decision == 'M':
        cat_attrs['d_master'] = input('Type the ID: ')
        cat_attrs['metoda_master'] = cur_method
        log.info('Found ID : {0} by a {1} method\n'.format(
            cat_attrs['d_master'], cat_attrs['metoda_master']))
    elif decision == 'r' or decision == 'R':
        cat_attrs['d_release'] = input('Type the ID: ')
        cat_attrs['metoda_release'] = cur_method
        log.info('Found ID : {0} by a {1} method\n'.format(
            cat_attrs['d_release'], cat_attrs['metoda_release']))
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_artist(cat_attrs, skip=False):
    log.info('Master: Artist\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(cat_attrs['artist']))
    if cat_attrs['artist'] == 'Various Artist':
        log.info('Method inappropriate for this album\n')
        return cat_attrs
    cur_method = 'by artist releases'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    artists_unf = d.search(cat_attrs['artist'], type='artist')
    log.info('FOUND {0} RESULTS'.format(len(artists_unf)))
    if len(artists_unf) > 2000:
        log.info('Too many artist, method skipped\n')
        return cat_attrs
    artists_ids = text_tools.find_match_artists(
        artists_unf, cat_attrs['artist'])
    if len(artists_ids) > 20:
        log.info('Too many artist, skipped method\n')
        return cat_attrs
    log.info('Getting artist ids list')
    releases_list = []
    for ids in artists_ids:
        log.info('***Connecting Discogs\tQuery: {0} as an artist id'.format(
            ids))
        artist_ = d.artist(ids)
        try:
            artist_releases = artist_.releases
        except discogs_client.exceptions.HTTPError as e:
            if e.status_code == 404:
                log.warning('\n\n\n\n\n\n\n\n\n')
                log.warning('{} - skipped artist'.format(e))
                log.warning('\n\n\n\n\n\n\n\n\n')
                continue
        log.info('FOUND {0} RESULTS'.format(len(artist_releases)))
        if len(artist_releases) > 8000:
            log.info('Too many releases, method skipped\n')
            cat_attrs['comment'] += 'too many releases of artist founded'
            return cat_attrs
        time.sleep(ratelimit)
        simple_releases = {}
        counter = 0
        for counter in range(len(artist_releases)):
            skip_flag = False
            if config.is_interrupted():
                return cat_attrs
            break_counter = 0
            log.debug('Starting step')
            while break_counter < 10:
                try:
                    release = artist_releases[counter]
                    log.info('{} Binded {} successfully'.format(
                        counter, release))
                except decoder.JSONDecodeError as e:
                    break_counter += 1
                    log.warning('{} nr {}\n'.format(e, break_counter))
                    time.sleep(ratelimit)
                    continue
                except IndexError as e:
                    skip_flag = True
                    log.warning('\n\n\n')
                    log.warning('Index Error forced skip')
                    log.warning('On {} item'.format(counter))
                    log.warning('\n\n\n')
                    break
                else:
                    break
            if break_counter == 10:
                log.warning('\n\n\n\n\n\n\n\n\n')
                log.warning('JSON Error forced skip')
                log.warning('On {} item'.format(counter))
                log.warning('\n\n\n\n\n\n\n\n\n')
                cat_attrs['comment'] += ' JSON Error'
                continue
            if skip_flag:
                log.warning('skipping step')
                continue
            if config.is_interrupted():
                return cat_attrs
            counter += 1
            log.debug('{} Id: {}, Title: {}'.format(
                counter, release.id, release.title))
            simple_releases[str(release.id)] = [release.title, type(release)]
            log.debug('Added {}'.format(simple_releases[str(release.id)][0]))
            if counter % 20 == 0:
                time.sleep(ratelimit)
            log.debug('Finishing step\n')
        log.info('releases list : {0}'.format(artist_releases))
        log.info('releases list length : {0}'.format(len(artist_releases)))
        log.info('releases list : {0}'.format(simple_releases))
        log.info('releases list length : {0}'.format(len(simple_releases)))
        counter = 0
        for rel_id, rel_data in simple_releases.items():
            counter += 1
            if config.is_interrupted():
                return cat_attrs
            log.info('{} checking {}'.format(counter, rel_data[0]))
            ratio = fuzz.ratio(rel_data[0], cat_attrs['album'])
            ratio_partial = fuzz.partial_ratio(rel_data[0], cat_attrs['album'])
            log.debug('Ratio : {0}'.format(ratio))
            log.debug('Ratio Partial : {0}'.format(ratio_partial))
            if ratio > 85 or ratio_partial > 98:
                if rel_data[1] == discogs_client.models.Master:
                    releases_list.append(d.master(rel_id))
                elif rel_data[1] == discogs_client.models.Release:
                    releases_list.append(d.release(rel_id))
                log.info('release {0} appended'.format(rel_data[0]))
            time.sleep(ratelimit)
    if config.is_interrupted():
        return cat_attrs
    counter = 0
    for release in releases_list:
        log.info('type of {0}, {1} release is {2}'.format(
            counter, release.title, type(release)))
        counter += 1
    if len(releases_list) == 1:
        cat_attrs = discogs_tools.save_m_or_r(
            cat_attrs, releases_list[0], cur_method)
    else:
        log.info('Too many matches, This case should be improved')
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_album_fuzz(cat_attrs, skip=False):
    log.info('Master: Fuzz Album\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(cat_attrs['album']))
    cur_method = 'm_by_album_fuzz'
    if cat_attrs['album'] == 'Unknown Album':
        log.info('Method inappropriate for this album\n')
        return cat_attrs
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    releases_unf = d.search(cat_attrs['album'], type='release')
    log.info('FOUND {0} RESULTS'.format(len(releases_unf)))
    if len(releases_unf) > 2000:
        log.info('Too many releases, method skipped\n')
        return cat_attrs
    filtered_releases = discogs_tools.filter_releases(cat_attrs, releases_unf)
    if config.is_interrupted():
        return cat_attrs
    final_list = discogs_tools.prefer_masters(filtered_releases)
    if len(filtered_releases) == 1:
        cat_attrs = discogs_tools.save_m_or_r(
            cat_attrs, final_list[0], cur_method)
    else:
        log.info('Found {} matches - need develop this method\n'.format(
            len(final_list)))
        cat_attrs['comment'] += 'AlbumFuzz-Too many matches'
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def m_by_album_fuzz_excl(cat_attrs, skip=False):
    log.info('Master: Fuzz Album with Replace\n')
    log.info('***Connecting Discogs\tQuery: {0}'.format(cat_attrs['album']))
    log.info('Looking for substring to exlude in : {0}\n'.format(
        cat_attrs['album']))
    cur_method = 'm_by_album_fuzz_excl'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    exclusion_flag = False
    for substr in text_tools.substr_to_exclude:
        if substr in cat_attrs['album']:
            log.info('Found substring to exclude: {0}\n'.format(substr))
            album_token = cat_attrs['album'].replace(substr, '')
            exclusion_flag = True
        else:
            continue
    if not exclusion_flag:
        log.info('Found nothing to exclude\n')
        return cat_attrs
    releases_unf = d.search(album_token, type='release')
    log.info('FOUND {0} RESULTS'.format(len(releases_unf)))
    if len(releases_unf) > 5000:
        log.info('Too many releases, method skipped\n')
        return cat_attrs
    filtered_releases = discogs_tools.filter_releases(cat_attrs, releases_unf)
    if config.is_interrupted():
        return cat_attrs
    final_list = discogs_tools.prefer_masters(filtered_releases)
    if len(filtered_releases) == 1:
        cat_attrs = discogs_tools.save_m_or_r(
            cat_attrs, final_list[0], cur_method)
    else:
        log.info('Found {} matches - need develop this method\n'.format(
            len(final_list)))
        cat_attrs['comment'] += 'AlbumFuzz-Too many matches'
    return cat_attrs


##########################################
# FINDING RELEASE
##########################################


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def r_by_album(cat_attrs, res_tresh, skip=False):
    log.info('Release: Album\n')
    log.info('Connecting Discogs\tQuery: {0}'.format(cat_attrs['album']))
    cur_method = 'Album'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    outcome = d.search(cat_attrs['album'], type='release')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    counter = 0
    for i in itertools.islice(outcome, 0, res_tresh):
        if config.is_interrupted():
            return cat_attrs
        if len(i.artists) > 1:
            log.debug('there is more than one artist in current release')
        counter += 1
        result = text_tools.rm_artist_num(i.artists[0].name), i.title
        cat_attrs, flag = discogs_tools.final_comparison(
            cat_attrs, result, i.id, cur_method, counter)
        if flag:
            break
        time.sleep(0.5 * ratelimit)
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs


@retry((requests.exceptions.ConnectionError,
        discogs_client.exceptions.HTTPError))
def r_by_token(cat_attrs, res_tresh, skip=False):
    log.info('Release: Token\n')
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    log.info('Connecting Discogs\tQuery: {0}'.format(token))
    cur_method = 'Token'
    if skip:
        cat_attrs['comment'] += 'skipped {}, '.format(cur_method)
        return cat_attrs
    outcome = d.search(token, type='release')
    log.info('FOUND {0} RESULTS'.format(len(outcome)))
    counter = 0
    for i in itertools.islice(outcome, 0, res_tresh):
        if config.is_interrupted():
            return cat_attrs
        counter += 1
        # tran_artist = cat_attrs['artist'].translate(punctuationremover)
        # tran_album = cat_attrs['album'].translate(punctuationremover)
        result = text_tools.rm_artist_num(i.artists[0].name), str(
            i.title).translate(punctuationremover)
        cat_attrs, flag = discogs_tools.final_comparison(
            cat_attrs, result, i.id, cur_method, counter)
        if flag:
            break
        time.sleep(ratelimit / 2)
    discogs_tools.log_summary(cat_attrs, cur_method)
    time.sleep(ratelimit)
    return cat_attrs
