import logging
import discogs_client
import time
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
from fuzzywuzzy import process


import modules.config as config

log = logging.getLogger('main.txttools')
ratelimit = config.ratelimit

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

substr_to_exclude = [
    'CDM',
    'Vinyl',
    '[Japan]',
    '(Japan Edition)',
    '(Maxi CD)',
    '(CDS)',
    '(Single)',
    '(CD Single)',
    'VLS',
    'Cd1',
    'CD1',
    '(CD 1)',
    ' - CD 1',
    '(Disc 1)',
    '[Disc 2]',
    'Cd2',
    'CD2',
    '(CD 2)',
    ' - CD 2',
    '(Disc 2)',
    '[Disc 2]',
    '[CD1]',
    '[CD2]',
    '[CD3]',
    '[CD4]',
    ' - EP',
    '[EP]',
    '(EP)',
    'WEB'
]


def rm_artist_num(artist):
    if artist[-1] == ')' and (artist[-3] or artist[-4]):
        log.info('splitting artist: {0}'.format(artist))
        artist, *_ = artist.split(' (')
    artist = artist.replace("*", "")
    return artist


def find_match_artists(artists, s_artist):
    log.info('Finding matching artists starts')
    names = {}
    counter = 0
    for artist in artists:
        log.debug('input artist: {0}'.format(artist.name))
        names[str(artist.id)] = rm_artist_num(artist.name)
        counter += 1
    log.info('there were {0} artist checked'.format(counter))
    log.debug('names dict: {0}'.format(names))
    extract_output = process.extract(s_artist, names)
    log.debug('output: {0}'.format(extract_output))
    ids = []
    for artist in extract_output:
        if artist[1] > 85:
            log.info('matched artist: {0}. {1}'.format(artist[2], artist[0]))
            ids.append(artist[2])
    return ids


def check_artist_similarity(tag_artist, rel_artist):
    rel_artist_ = rm_artist_num(rel_artist)
    log.debug('Finding checking artists starts')
    log.debug('tag artist: {0}'.format(tag_artist))
    log.debug('release artist: {0}, corrected to: {1}'.format(
        rel_artist, rel_artist_))
    ratio_full = fuzz.ratio(tag_artist, rel_artist_)
    ratio_part = fuzz.partial_ratio(tag_artist, rel_artist_)
    log.debug('Ratio : {0}'.format(ratio_full))
    log.debug('Ratio Partial : {0}'.format(ratio_part))
    if ratio_full > 85 or ratio_part > 96:
        return True
    else:
        return False


def check_album_similarity(tag_album, rel_title):
    tag_album_ = fm_album_suffixes(tag_album)
    log.debug('Checking album title similarity')
    log.debug('tag album: {}, corrected to: {}'.format(tag_album, tag_album_))
    ratio_full = fuzz.ratio(tag_album_, rel_title)
    ratio_part = fuzz.partial_ratio(tag_album_, rel_title)
    log.debug('Ratio : {0}'.format(ratio_full))
    log.debug('Ratio Partial : {0}'.format(ratio_part))
    time.sleep(0.7 * ratelimit)
    if ratio_full > 80 or ratio_part > 90:
        return True
    else:
        return False


def fm_album_suffixes(album):
    album, *_ = album.split(' (')
    album, *_ = album.split(' [')
    return album


def find_longest_substring(str1, str2):
    '''Getting the longest substring of two inputs strings
    '''
    log.debug(
        'find_longest_substring comparing pair: {0} and {1}'.format(
            str1, str2))
    seqMatch = SequenceMatcher(None, str1, str2)
    match = seqMatch.find_longest_match(0, len(str1), 0, len(str2))
    if match.size != 0:
        selection = str1[match.a: match.a + match.size]
        log.debug('Selected : {0}'.format(selection))
        return selection
    else:
        log.debug('Common substring not found\n')


def handle_multiple_artist(alist):
    '''Choose the one substring common for all input list elements
    '''
    comparisionlist = []
    substrings = []
    log.info('Substring Finder Function starts')
    log.debug('Input : {0}\n'.format(alist))
    for i in range(len(alist) - 1):
        match = find_longest_substring(alist[i], alist[i + 1])
        if match not in substrings and match is not None and len(match) > 1:
            substrings.append(match)
    log.info('Existing substrings : {0}'.format(substrings))
    for j in substrings:
        log.debug('Checking if {0} has a substring with all'.format(j))
        flag = 0
        for k in alist:
            if find_longest_substring(j, k) != j:
                flag = 1
                log.debug('{0} is not a match'.format(j))
                break
        if flag == 0:
            log.debug('{0} has a match with all files in dir'.format(j))
            log.info('***Connecting Discogs\t\t\tQuery: <{0}>'.format(j))
            try:
                firstshot = d.search(j, type='artist')[0].name
            except IndexError as e:
                log.warning('no artist {0} found'.format(match))
                firstshot = ''
            else:
                log.debug('Found an artist in discogs database : {0}'.format(
                    firstshot))
            log.debug(
                'Checking if my match {0} fits to {1}'.format(
                    j, firstshot))
            if j == firstshot:
                comparisionlist.append(j)
            log.debug(
                'Found match : {0}, that exists on Discogs'.format(match))
            time.sleep(ratelimit)
    if len(comparisionlist) == 1:
        log.info(
            'Substring Finder found match : {0}\n'.format(comparisionlist[0]))
        return comparisionlist[0]
    else:
        log.info('Substring Finder did not found any match\n')
        return 'Various Artist'
