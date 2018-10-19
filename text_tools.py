import logging
import discogs_client
import time
from difflib import SequenceMatcher

import config

log = logging.getLogger('main.txttools')
ratelimit = config.ratelimit

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

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