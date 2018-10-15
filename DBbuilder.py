from datetime import datetime
import sqlite3
import os
import sys
import taglib
import discogs_client
import itertools
from difflib import SequenceMatcher
import string
import pickle
import time
import logging
import signal
import requests


#############################################
# CONFIGURATION SECTION
#############################################


log = logging.getLogger()
log.handlers = []

log = logging.getLogger('DB Builder')
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
fh = logging.FileHandler(datetime.now().strftime(
    'logs\\log-%Y.%m.%d-%H.%M.log'), 'w')
fh.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s-%(levelname)s: %(message)s',
                              datefmt='%Y.%m.%d %H:%M:%S')

ch.setFormatter(formatter)
fh.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)

interrupted = False

punctuationremover = str.maketrans('', '', string.punctuation)

if sys.platform == 'linux':
    log.info('OS : Linux')
    inputpath = "/home/kuba/Muzyka"
    slasher = '/'
    catalogcachefile = 'lin_catalogs.dat'
    ratelimit = 2
    databasefilename = 'lin_database.db'
if sys.platform == 'win32':
    log.info('OS : Windows')
    inputpath = "D:\\++WORKZONE++"
    slasher == '\\'
    catalogcachefile = 'win_catalogs.dat'
    ratelimit = 1
    databasefilename = 'database.db'

extlist = ['.mp3', '.ogg', '.flac', '.wav', '.wma', '.ape']

con = sqlite3.connect(databasefilename)
con.row_factory = sqlite3.Row
cursor = con.cursor()


def signal_handler(signal, frame):
    global interrupted
    interrupted = True


signal.signal(signal.SIGINT, signal_handler)


d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

#############################################
# DATABASE DEFINITIONS
#############################################

cursor.executescript("""
    CREATE TABLE IF NOT EXISTS katalogi (
        id INTEGER PRIMARY KEY ASC,
        sciezka varchar(400) NOT NULL,
        artysta varchar(250) NOT NULL,
        album varchar(250) NOT NULL,
        data DATE,
        jakosc INTEGER NOT NULL,
        katalog varchar(200),
        discogs varchar(250),
        metoda varchar(100)
    )
    """)
cursor.executescript("""
    CREATE TABLE IF NOT EXISTS pliki (
        id INTEGER PRIMARY KEY ASC,
        sciezka varchar(400) NOT NULL,
        artysta varchar(250),
        tytul varchar(400),
        album varchar(250),
        jakosc INTEGER NOT NULL,
        katalog_id INTEGER NOT NULL,
        FOREIGN KEY(katalog_id) REFERENCES katalogi(id)
    )""")


#############################################
# PREPARATIONS FUNCTIONS DEFINITIONS
#############################################


def Crawler(path=inputpath):
    '''Loads or creates a list od every catalog in path containg audio file
    '''
    result = []
    log.info('Crawler Function starts')
    if os.path.exists(catalogcachefile):
        log.debug('Catalog Cache File founded')
        with open(catalogcachefile, 'rb') as file:
            result = pickle.load(file)
    else:
        log.debug('Catalog Cache File not found')
        log.debug('Starting reading library')
        xcounter = 0
        for x in os.walk(path):
            xcounter += 1
            for i in os.listdir(x[0]):
                if AudioFile(i):
                    result.append(x[0])
                    break
            if xcounter % 500 == 0:
                log.debug('Already checked {0} directories'.format(xcounter))
        log.info('Library reading ended with {0} directories'.format(xcounter))
        with open(catalogcachefile, 'wb') as file2:
            pickle.dump(result, file2)
    log.info('Crawler Function ended\n\n')
    return result


def AudioFile(file):
    '''checks if input file has extension caontained on audio file extensions list
    '''
    filename, extension = os.path.splitext(file)
    if extension in extlist:
        return True
    else:
        return False


#############################################
# CATALOG OPERATIONS FUNCTIONS DEFINITIONS
#############################################


def longestSubstring(str1, str2):
    '''Getting the longest substring of two inputs strings
    '''
    log.debug(
        'longestSubstring comparing pair: {0} and {1}'.format(str1, str2))
    seqMatch = SequenceMatcher(None, str1, str2)
    match = seqMatch.find_longest_match(0, len(str1), 0, len(str2))
    if match.size != 0:
        selection = str1[match.a: match.a + match.size]
        log.debug('Selected : {0}'.format(selection))
        return selection
    else:
        log.debug('Common substring not found\n')


def substringFinder(alist):
    '''Choose the one substring common for all input list elements
    '''
    comparisionlist = []
    substrings = []
    log.info('Substring Finder Function starts')
    log.debug('Input : {0}\n'.format(alist))
    for i in range(len(alist) - 1):
        match = longestSubstring(alist[i], alist[i + 1])
        if match not in substrings and match is not None and len(match) > 1:
            substrings.append(match)
    log.info('Existing substrings : {0}'.format(substrings))
    for j in substrings:
        flag = 0
        for k in alist:
            if longestSubstring(j, k) != j:
                flag = 1
        if flag == 0:
            log.info('***Connecting Discogs\nQuery: {0}'.format(match))
            substringsearch = d.search(match, type='artist')
            firstshot = substringsearch[0].name
            log.debug(
                'Checking if my match {1} fits to {0}'.format(
                    firstshot, match))
            if match == firstshot:
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


def CatalogWorker(path):
    # print('w tej funkcji maja sie znaleźć działania nad katalogiem')
    albumlist = []
    artistlist = []
    filesidlist = []
    log.info('Catalog Worker starts : {0}'.format(path))
    files = [f for f in os.listdir(path) if (os.path.isfile(os.path.join(path,f)))]
    for i in files:
        if AudioFile(i):
            fileid = FileWorker(path + slasher + i, albumlist, artistlist)
            filesidlist.append(fileid)
    token = RecognizeCatalog(albumlist, artistlist, path)
    if (token['artist'] != 'Unknown Artist' and
            token['album'] != 'Unknown Album'):
        discogsid = DiscogsID(token['artist'], token['album'], path)
    else:
        log.debug('\n\nNie rozpoznano albumu')
        discogsid = ''
    cursor.execute(
        'INSERT INTO katalogi VALUES(NULL, ?, ?, ?, ?);',
        (path, token['artist'], token['album'], discogsid))
    catalogid = cursor.lastrowid
    for j in filesidlist:
        cursor.execute(
            'UPDATE pliki SET katalog_id=? WHERE id=?', (catalogid, j))
    con.commit()
    log.info(
        'Worked on :\nCatalogID:\t{0}\nArtysta:\t{1}\nAlbum:\t{2}\nWork done\
        \n\n'.format(catalogid, token['artist'], token['album']))


def FileWorker(path, albumlist, artistlist):
    # print('w tej funkcji maja byc wykonywane dzialania nad plikiem')
    log.info('\nWorking on file : {0}'.format(path))
    try:
        song = taglib.File(path)
    except OSError:
        log.warning('OS Error')
        log.warning(
            'np nieakceptowalne znaki\
             - usuniete od pytaglib wersji 1.4.2')
        albumtag = ''
        artisttag = ''
        titletag = ''
        cursor.execute(
            'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?);',
            (path, artisttag, titletag, albumtag, 0))
        fileid = cursor.lastrowid
        con.commit()
        return fileid
    try:
        albumtag = song.tags['ALBUM'][0]
        if albumtag not in albumlist:
            albumlist.append(albumtag)
    except KeyError as e:
        log.warning('KeyError: {0}'.format(e))
        log.warning('pole ALBUM błędne')
        albumtag = ''
    except IndexError:
        log.warning('IndexError')
        log.warning('pole ALBUM puste (?)')
        albumtag = ''
    try:
        artisttag = song.tags['ARTIST'][0]
        if artisttag not in artistlist:
            artistlist.append(artisttag)
    except KeyError as e:
        log.warning('KeyError: {0}'.format(e))
        log.warning('pole artist błędne lub puste')
        artisttag = ''
    except IndexError:
        log.warning('IndexError')
        log.warning('pole ARTIST puste (?)')
        artisttag = ''
    try:
        titletag = song.tags['TITLE'][0]
    except KeyError as e:
        log.warning('KeyError: {0}'.format(e))
        log.warning('pole title błędne lub puste')
        titletag = ''
    except IndexError:
        log.warning('IndexError')
        log.warning('pole TITLE puste (?)')
        titletag = ''
    cursor.execute(
        'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?);',
        (path, artisttag, titletag, albumtag, 0))
    fileid = cursor.lastrowid
    con.commit()
    return fileid
    # utwórz rekord w db encja Pliki


def RecognizeCatalog(albumlist, artistlist, path):
    log.info('Recognize Catalog function starts')
    result = {}
    if len(albumlist) == 1:
        result['album'] = albumlist[0]
    else:
        result['album'] = 'Unknown Album'
    if len(artistlist) == 1:
        result['artist'] = artistlist[0]
    elif len(albumlist) == 1:
        result['artist'] = substringFinder(artistlist)
    else:
        result['artist'] = 'Unknown Artist'
    log.info('Recognize Catalog function ends\n\n')
    return result


#############################################
# DISCOGS DATABASE HANDLERS DEFINITIONS
#############################################


def DiscogsID(artist, album, path):
    log.info('\n\n***Searching for {0} - {1}'.format(artist, album))
    token = artist + ' - ' + album
    result = ''
    # ile wyników z zapytania brać pod uwagę
    res_tresh = 50
    try:
        log.info('\nmetoda Album')
        log.info('Connecting Discogs\nQuery: {0}'.format(album))
        master_album = d.search(album, type='master')
        for i in itertools.islice(master_album, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            if martist == artist and malbum == album:
                log.info('Znaleziono ID z listy Album')
                log.info('{0}'.format(i.id))
                result = i.id
                break
        time.sleep(ratelimit)
        i = 0
        if result == '':
            log.info('\nmetoda Token')
            log.info('Connecting Discogs\n Query: {0}'.format(token))
            master_token = d.search(token, type='master')
            for i in itertools.islice(master_token, 0, res_tresh):
                log.info('{0}'.format(i.title))
                masterlist = i.title.split(' - ')
                martist = masterlist[0]
                malbum = masterlist[1]
                log.info('Comparing {0} - {1}'.format(martist, malbum))
                if martist == artist and malbum == album:
                    log.info('Znaleziono ID z listy Token')
                    log.info('{0}'.format(i.id))
                    result = i.id
                    break
        time.sleep(ratelimit)
        i = 0
        if result == '':
            log.info('\nmetoda Variations')
            log.info('Connecting Discogs\nQuery: {0}'.format(artist))
            varartist = d.search(artist, type='artist')
            variations = varartist[0].name_variations
            log.info('{0}  aliases : {1}'.format(varartist[0], variations))
            if variations is not None:
                for k in variations:
                    if result != '':
                        break
                    tokenvar = k + ' - ' + album
                    log.info('Connecting Discogs\nQuery: {0}'.format(tokenvar))
                    master_variations = d.search(tokenvar, type='master')
                    log.info('tried: {0}'.format(tokenvar))
                    for i in itertools.islice(master_variations, 0, res_tresh):
                        masterlist = i.title.split(' - ')
                        martist = masterlist[0].split('*')[0]
                        martist = martist.split(' (')[0]
                        malbum = masterlist[1]
                        log.info('Comparing {0} - {1}'.format(martist, malbum))
                        if martist == k and malbum == album:
                            log.info('Znaleziono ID z listy Variations')
                            log.info('{0}'.format(i.id))
                            result = i.id
                            break
                    time.sleep(ratelimit)
        time.sleep(ratelimit)
        i = 0
        if result == '':
            log.info('\nmetoda Release')
            log.info('Connecting Discogs\nQuery: {0}'.format(album))
            release = d.search(album, type='release')
            for i in itertools.islice(release, 0, res_tresh):
                masterlist = i.title.split(' - ')
                martist = masterlist[0]
                malbum = masterlist[1]
                log.info('Comparing {0} - {1}'.format(martist, malbum))
                if martist == artist and malbum == album:
                    log.info('Znaleziono ID z listy Release')
                    log.info('{0}'.format(i.id))
                    result = i.id
                    break
        time.sleep(ratelimit)
        if result == '':
            log.info('\nmetoda Release2')
            token_improved = token
            log.info('Connecting Discogs\nQuery: {0}'.format(token_improved))
            release2 = d.search(token_improved, type='release')
            for i in itertools.islice(release2, 0, res_tresh):
                masterlist = i.title.split(' - ')
                martist = masterlist[0].translate(punctuationremover)
                malbum = masterlist[1].translate(punctuationremover)
                tranartist = artist.translate(punctuationremover)
                tranalbum = album.translate(punctuationremover)
                log.info('Comparing {0} - {1}'.format(martist, malbum))
                log.info('With {0} - {1}'.format(tranartist, tranalbum))
                if martist == tranartist and malbum == tranalbum:
                    log.info('Znaleziono ID z listy Release2')
                    log.info('{0}'.format(i.id))
                    result = i.id
                    break
        time.sleep(ratelimit)
    except IndexError:
        log.warning('przewano szukanie ID')

    return result


#############################################
# MAIN PROGRAM SECTION
#############################################


cataloglist = Crawler()

STEPS = len(cataloglist)

try:
    for j in range(STEPS):
        try:
            cursor.execute(
                'SELECT sciezka FROM katalogi WHERE sciezka = ?',
                (cataloglist[j],))
            existingrecord = cursor.fetchone()[0]
            log.info('Skipped {0}'.format(existingrecord))
            continue
        except TypeError as NoneType:
            while True:
                try:
                    CatalogWorker(cataloglist[j])
                    time.sleep(ratelimit)
                except requests.exceptions.ConnectionError as e:
                    log.warning('{}'.format(e))
                    log.warning(
                        'Connection Broken. Trying to connect in 120 seconds')
                    time.sleep(120)
                    if interrupted:
                        print("Exiting Script")
                        j = range(STEPS)[-1]
                        break
                    continue
                break
            if interrupted:
                print("Exiting Script")
                j = range(STEPS)[-1]
                break
        except TypeError as e:
            log.warning(
                '{0}\nnot all arguments converted during formatting'.format(e))
        except ConnectionError as e:
            log.warning('{}'.format(e))
            time.sleep(600)
except SyntaxError:
    log.warning('SyntaxError')
# except IndexError:
#     print('IndexError')
