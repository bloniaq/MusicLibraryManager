import logging
import discogs_client
import time

import config
import text_tools

log = logging.getLogger('main.dgs_tls')


ratelimit = config.ratelimit
checklist = config.discogs_checklist

config.signal_trig()


def is_found(dictionary, *keys):
    flag = False
    for key in keys:
        if key in dictionary:
            if dictionary[key] != '':
                flag = True
    return flag


def filter_releases(cat_attrs, releases_unf):
    filtered_list = []
    log.info('Starting filtering founded {0} releases'.format(
        len(releases_unf)))
    release_count = 0
    for release in releases_unf:
        release_count += 1
        if config.is_interrupted():
            return filtered_list
        release_title = get_title_m_or_r(release)
        if not text_tools.check_album_similarity(
                cat_attrs['album'], release_title):
            log.info('{} Release <{}> isn\'t matching to <{}>'.format(
                release_count, release_title, cat_attrs['album']))
            continue
        if len(release.artists) > 1:
            log.warning('{} Too many artist in {}'.format(
                release_count, release.title))
            for counter in range(len(release.artists)):
                log.info('{} Artist: {}'.format(
                    counter + 1, release.artists[counter].name))
                time.sleep(0.5 * ratelimit)
            log.info('Checking skipped, need rework\n')
            continue
        if release.artists[0].name == "Unknown Artist":
            log.warning('{} Artist Unknown'.format(release_count))
            log.warning('Artist: <{}>, Release: <{}>\n'.format(
                release.artists[0].name, release.title))
            continue
        log.info('{} Checking similarity for {} and (from tags): {}'.format(
            release_count, release.artists[0].name, cat_attrs['artist']))
        if text_tools.check_artist_similarity(
                cat_attrs['artist'], release.artists[0].name):
            filtered_list.append(release)
            log.info('')
            log.info('Release {0} appended, that was {1} result\n'.format(
                release, release_count))
        time.sleep(0.7 * ratelimit)
    log.info('filtered releases: {1} - {0}\n'.format(
        filtered_list, len(filtered_list)))
    return filtered_list


def get_title_m_or_r(discogs_object):
    if isinstance(discogs_object, discogs_client.models.Master):
        name = discogs_object.title.split(' - ')
        log.debug(
            'Func get_title_m_or_r found {} as a title of {} object'.format(
                name[1], discogs_object))
        return name[1]
    if isinstance(discogs_object, discogs_client.models.Release):
        log.debug(
            'Func get_title_m_or_r found {} as a title of {} object'.format(
                discogs_object.title, discogs_object))
        return discogs_object.title


def final_comparison(cat_attrs, tokens, i_id, method, c=0):
    flag = False
    log.info('{} Comparing {} - {} from discogs'.format(
        c, tokens[0], tokens[1]))
    log.info('to {} - {} from tags'.format(
        c, cat_attrs['artist'], cat_attrs['album']))
    if tokens[0] == cat_attrs['artist'] and tokens[1] == cat_attrs['album']:
        flag = True
        cat_attrs['metoda_master'] = method
        cat_attrs['d_master'] = i_id
        log.info('Found ID : {0} by a {1} method\n'.format(
            i_id, cat_attrs['metoda_master']))
    return cat_attrs, flag


def log_summary(cat_attrs, cur_method):
    try:
        log.info('values on output: {0}, {1}\n'.format(
            cat_attrs['metoda_master'], cat_attrs['d_master']))
    except BaseException as e:
        log.warning('no values on output, method: {}\n'.format(cur_method))


def save_m_or_r(cat_attrs, list_item, method):
    log.info('Recognizing type of founded item')
    log.info('Item: {}'.format(list_item))
    log.info('Type: {}'.format(type(list_item)))
    if isinstance(list_item, discogs_client.models.Master):
        cat_attrs['metoda_master'] = method
        cat_attrs['d_master'] = list_item.id
        log.info('Found ID : {0} by a {1} method\n'.format(
            list_item.id, cat_attrs['metoda_master']))
    if isinstance(list_item, discogs_client.models.Release):
        cat_attrs['metoda_release'] = method
        cat_attrs['d_release'] = list_item.id
        log.info('Found ID : {0} by a {1} method\n'.format(
            list_item.id, cat_attrs['metoda_release']))
    return cat_attrs


def prefer_masters(matches_list):
    final_list = []
    for release in matches_list:
        if config.is_interrupted():
            return []
        if release.master is not None:
            if release.master not in final_list:
                log.info('for {0} id - {1} found master: {2}'.format(
                    release.id, release.title, release.master.id))
                final_list.append(release.master)
        elif release not in final_list:
            log.info('appended release: {0} - {1}'.format(
                release.artists, release.title))
            final_list.append(release)
    return final_list
