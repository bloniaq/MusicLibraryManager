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

if sys.platform == 'linux':
    inputpath = "/home/kuba/Muzyka"
    slasher = '/'
    catalogcachefile = 'lin_catalogs.dat'
    ratelimit = 2
    databasefilename = 'lin_database.db'
if sys.platform == 'win32':
    inputpath = "D:\\++WORKZONE++"
    slasher == '\\'
    catalogcachefile = 'win_catalogs.dat'
    ratelimit = 1
    databasefilename = 'database.db'

log = logging.getLogger()
log.handlers = []

log = logging.getLogger('DB Builder')
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = logging.FileHandler(datetime.now().strftime(
    'logs' + slasher + 'log-%Y.%m.%d-%H.%M.log'), 'w', 'utf-8')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s-%(levelname)s: %(message)s',
                              datefmt='%Y.%m.%d %H:%M:%S')

ch.setFormatter(formatter)
fh.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)

interrupted = False

punctuationremover = str.maketrans('', '', string.punctuation)

extlist = ['.mp3', '.ogg', '.flac', '.wav', '.wma', '.ape']
imgextlist = ['.jpg', '.jpeg', '.png']

con = sqlite3.connect(databasefilename)
con.row_factory = sqlite3.Row
cursor = con.cursor()

if sys.platform == 'linux':
    log.info('OS : Linux')
if sys.platform == 'win32':
    log.info('OS : Windows')


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
        discogs varchar(250),
        jakosc INTEGER NOT NULL,
        dlugosc INTEGER,
        katalog varchar(200),
        metoda_did varchar(100),
        obrazki INTEGER
    )
    """)

cursor.executescript("""
    CREATE TABLE IF NOT EXISTS pliki (
        id INTEGER PRIMARY KEY ASC,
        sciezka varchar(400) NOT NULL,
        artysta varchar(250),
        tytul varchar(400),
        katalog_id INTEGER NOT NULL,
        album varchar(250),
        dlugosc INTEGER,
        jakosc INTEGER NOT NULL,
        FOREIGN KEY(katalog_id) REFERENCES katalogi(id)
    )""")


#############################################
# PREPARATIONS FUNCTIONS DEFINITIONS
#############################################


def catalog_collector(path=inputpath):
    '''Loads or creates a list od every catalog in path containg audio file
    '''
    result = []
    log.info('catalog_collector Function starts')
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
                if audiofile_check(i):
                    result.append(x[0])
                    break
            if xcounter % 500 == 0:
                log.debug('Already checked {0} directories'.format(xcounter))
        log.info('Library reading ended with {0} directories'.format(xcounter))
        with open(catalogcachefile, 'wb') as file2:
            pickle.dump(result, file2)
    log.info('catalog_collector Function ended\n\n')
    return result


def audiofile_check(file):
    '''checks if input file has extension caontained on audio file extensions list
    '''
    filename, extension = os.path.splitext(file)
    if extension in extlist:
        return True
    else:
        return False


def imagefile_check(file):
    '''checks if input file has extension contained in image file extensions list
    '''
    filename, extension = os.path.splitext(file)
    if extension in imgextlist:
        return True
    else:
        return False


#############################################
# CATALOG OPERATIONS FUNCTIONS DEFINITIONS
#############################################


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


def find_substring_in_list(alist):
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
        flag = 0
        for k in alist:
            if find_longest_substring(j, k) != j:
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


def catalog_crawler(path):
    '''returns data got from audiofiles tags'''
    files_attributes_list = []
    image_count = 0
    log.info('Catalog Crawler starts')
    files = [f for f in os.listdir(path)
             if (os.path.isfile(os.path.join(path, f)))]
    log.debug('Files found in catalog : {0}'.format(files))
    log.info('Starting loop through files\n')
    for i in files:
        if audiofile_check(i):
            files_attributes_list.append(
                file_crawler(path + slasher + i))
        if imagefile_check(i):
            image_count += 1
    log.info('\n')
    log.info('Loop through files ends succesfully\n')
    catalog_attributes = determine_cat_attr(files_attributes_list)
    catalog_attributes['img_count'] = image_count
    catalog_attributes['path'] = path
    log.debug('returns {0}'.format(catalog_attributes))
    log.info('Catalog Crawler ends,\n')
    return catalog_attributes, files_attributes_list


def file_crawler(filepath):
    '''gets data from audiofile tags and returns it in dictionary'''
    log.info('File Crawler for starts for file : {0}'.format(filepath))
    file_attributes = {}
    file_attributes['path'] = filepath
    try:
        audiofile = taglib.File(filepath)
    except OSError as e:
        log.warning('OS Error : {0}'.format(e))
        file_attributes['album'] = '__OSError'
        file_attributes['artist'] = '__OSError'
        file_attributes['title'] = '__OSError'
    else:
        log.debug('Handling file by taglib - correct')
    try:
        try:
            file_attributes['album'] = audiofile.tags['ALBUM'][0]
        except KeyError as e:
            log.warning('KeyError: {0}'.format(e))
            file_attributes['album'] = '__KeyError'
        except IndexError as e:
            log.warning('IndexError: {0}'.format(e))
            file_attributes['album'] = '__IndexError'
        try:
            file_attributes['artist'] = audiofile.tags['ARTIST'][0]
        except KeyError as e:
            log.warning('KeyError: {0}'.format(e))
            file_attributes['artist'] = '__KeyError'
        except IndexError as e:
            log.warning('IndexError: {0}'.format(e))
            file_attributes['artist'] = '__IndexError'
        try:
            file_attributes['title'] = audiofile.tags['TITLE'][0]
        except KeyError as e:
            log.warning('KeyError: {0}'.format(e))
            file_attributes['title'] = '__KeyError'
        except IndexError as e:
            log.warning('IndexError: {0}'.format(e))
            file_attributes['title'] = '__IndexError'
        try:
            file_attributes['date'] = audiofile.tags['DATE'][0]
        except KeyError as e:
            log.warning('KeyError: {0}'.format(e))
            file_attributes['date'] = '__KeyError'
        except IndexError as e:
            log.warning('IndexError: {0}'.format(e))
            file_attributes['date'] = '__IndexError'
        file_attributes['length'] = audiofile.length
        file_attributes['bitrate'] = audiofile.bitrate
    except Exception as e:
        log.error('!!! THERE IS AN ERROR WHILE COLLECTING DATA FROM A FILE')
        log.error('{0}\n'.format(e))
    else:
        log.info('All file attributes collected correct')
        log.info('{0}\n'.format(file_attributes))
    return file_attributes


def determine_cat_attr(f_attr_list):
    '''tries to recognize catalog attributes based on files attributes'''
    log.info('Determine Catalog Attributes function starts\n')
    cat_attr = {}

    # Album

    log.debug('Tries to guess Album name')
    cat_album_tags = []
    for i in f_attr_list:
        if (i['album'][:2] != '__' and
                i['album'] != '' and
                i['album'] not in cat_album_tags):
            cat_album_tags.append(i['album'])
    log.debug('Found {0} proposition(s) : {1}'.format(
        len(cat_album_tags), cat_album_tags))
    if len(cat_album_tags) == 1:
        log.debug('Album found : {0}'.format(cat_album_tags[0]))
        cat_attr['album'] = cat_album_tags[0]
    else:
        log.debug('Album unreckognized')
        cat_attr['album'] = 'Unknown Album'

    # Artist

    log.debug('Tries to guess Artist name')
    cat_artist_tags = []
    for i in f_attr_list:
        try:
            if (i['artist'][:2] != '__' and
                    i['artist'] not in cat_artist_tags):
                cat_artist_tags.append(i['artist'])
        except KeyError as e:
            log.warning('KeyError : {0}'.format(e))
    log.debug('Found {0} proposition(s) : {1}'.format(
        len(cat_artist_tags), cat_artist_tags))
    if len(cat_artist_tags) == 0:
        log.debug('Artist unreckognized')
        cat_attr['artist'] = 'Unknown Artist'
    elif len(cat_artist_tags) == 1:
        log.debug('Artist found : {0}'.format(cat_artist_tags[0]))
        cat_attr['artist'] = cat_artist_tags[0]
    elif len(cat_artist_tags) > 1 and len(cat_album_tags) == 1:
        log.debug('Many Artists found in this catalog')
        cat_attr['artist'] = find_substring_in_list(cat_artist_tags)
        log.debug('Identified {0} as an artist'.format(cat_attr['artist']))
    else:
        log.debug('It seems catalog is a container not an album')
        cat_attr['artist'] = 'Unknown Artist'

    # Date

    log.debug('Tries to guess date of album')
    cat_date_tags = []
    for i in f_attr_list:
        try:
            if i['date'] not in cat_date_tags:
                cat_date_tags.append(i['date'])
        except KeyError as e:
            cat_date_tags.append('')
    if len(cat_date_tags) == 1:
        log.debug('Date recognized : {0}'.format(cat_date_tags[0]))
        cat_attr['date'] = cat_date_tags[0]
    else:
        log.debug('Date unreckognized')
        cat_attr['date'] = ''

    # Average Bitrate

    log.debug('Computing average bitrate of files')
    bitrate_sum = 0
    for i in f_attr_list:
        bitrate_sum += i['bitrate']
    cat_attr['avg_bitrate'] = bitrate_sum / len(f_attr_list)

    # Total Length

    log.debug('Computing total length of files')
    tot_len = 0
    for i in f_attr_list:
        tot_len += i['length']
    cat_attr['total_length'] = tot_len

    # Catalog Symbol -- IN FUTURE

    cat_attr['catalog_mark'] = ''

    # Summary

    log.info('reckognized Catalog Attributes:\nArtist: {0}\
        \nAlbum: {1}\nDate: {2}\nAvgBitrate: {3}\nCatalogSym{4}\n\n'.format(
        cat_attr['artist'], cat_attr['album'], cat_attr['date'],
        cat_attr['avg_bitrate'], cat_attr['catalog_mark']))
    return cat_attr


#############################################
# DISCOGS DATABASE HANDLERS DEFINITIONS
#############################################


def find_album_d_id(cat_attrs, f_attrs_list):
    '''tries to return master or release id from discogs db based on inputs'''
    log.info('Finding Album DiscogsID started')
    log.info('Working on {0}'.format(cat_attrs['path']))
    token = cat_attrs['artist'] + ' - ' + cat_attrs['album']
    result = ''
    cat_attrs['metoda_did'] = ''
    # ile wyników z zapytania brać pod uwagę
    res_tresh = 50
    try:
        log.info('metoda Album')
        log.info('Connecting Discogs\nQuery: {0}'.format(cat_attrs['album']))
        master_album = d.search(cat_attrs['album'], type='master')
        for i in itertools.islice(master_album, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            log.info('Comparing {0} - {1}'.format(martist, malbum))
            if martist == cat_attrs['artist'] and malbum == cat_attrs['album']:
                log.info('Znaleziono ID z listy Album')
                log.info('{0}'.format(i.id))
                cat_attrs['metoda_did'] = 'Album'
                result = i.id
                break
        time.sleep(ratelimit)
        i = 0
        if result == '':
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
                    cat_attrs['metoda_did'] = 'Token'
                    result = i.id
                    break
        time.sleep(ratelimit)
        i = 0
        if result == '':
            log.info('metoda Variations')
            log.info('Connecting Discogs\nQuery: {0}'.format(
                cat_attrs['artist']))
            varartist = d.search(cat_attrs['artist'], type='artist')
            variations = varartist[0].name_variations
            log.info('{0}  aliases : {1}'.format(varartist[0], variations))
            if variations is not None:
                for k in variations:
                    if result != '':
                        break
                    tokenvar = k + ' - ' + cat_attrs['album']
                    log.info('Connecting Discogs\nQuery: {0}'.format(tokenvar))
                    master_variations = d.search(tokenvar, type='master')
                    log.info('tried: {0}'.format(tokenvar))
                    for i in itertools.islice(master_variations, 0, res_tresh):
                        masterlist = i.title.split(' - ')
                        martist = masterlist[0].split('*')[0]
                        martist = martist.split(' (')[0]
                        malbum = masterlist[1]
                        log.info('Comparing {0} - {1}'.format(martist, malbum))
                        if martist == k and malbum == cat_attrs['album']:
                            log.info('Znaleziono ID z listy Variations')
                            log.info('{0}'.format(i.id))
                            result = i.id
                            cat_attrs['metoda_did'] = 'Variations'
                            break
                    time.sleep(ratelimit)
        time.sleep(ratelimit)
        i = 0
        if result == '':
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
                    cat_attrs['metoda_did'] = 'Release'
                    result = i.id
                    break
        time.sleep(ratelimit)
        if result == '':
            log.info('metoda Release2')
            token_improved = token
            log.info('Connecting Discogs\nQuery: {0}'.format(token_improved))
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
                    cat_attrs['metoda_did'] = 'Release2'
                    result = i.id
                    break
        time.sleep(ratelimit)
    except IndexError as e:
        log.warning('przewano szukanie ID z powodu {0}'.format(e))
    cat_attrs['d_id'] = result

    return cat_attrs, f_attrs_list


#############################################
# DB HANDLING
#############################################


def save_to_db(cat_attrs, f_attrs_list):
    log.info('Save to DB started')
    cursor.execute(
        'INSERT INTO katalogi VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
        (cat_attrs['path'],
         cat_attrs['artist'],
         cat_attrs['album'],
         cat_attrs['date'],
         cat_attrs['d_id'],
         cat_attrs['avg_bitrate'],
         cat_attrs['total_length'],
         cat_attrs['catalog_mark'],
         cat_attrs['metoda_did'],
         cat_attrs['img_count']
         ))
    log.info('Catalog data inserted successfully')
    catalogid = cursor.lastrowid
    for i in range(len(f_attrs_list)):
        log.debug('Tries to insert : {0}'.format(f_attrs_list[i]))
        cursor.execute(
            'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?, ?, ?);',
            (f_attrs_list[i]['path'],
             f_attrs_list[i]['artist'],
             f_attrs_list[i]['title'],
             catalogid,
             f_attrs_list[i]['album'],
             f_attrs_list[i]['length'],
             f_attrs_list[i]['bitrate']
             ))
    log.info('Catalog data inserted successfully')
    con.commit()
    log.info('Data Commited')

#############################################
# MAIN PROGRAM SECTION
#############################################


cataloglist = catalog_collector()

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
                    crawl_res_c, craw_res_f = catalog_crawler(cataloglist[j])
                    if (crawl_res_c['artist'] != 'Unknown Artist' and
                            crawl_res_c['album'] != 'Unknown Album'):
                        req_res_c, req_res_f = find_album_d_id(
                            crawl_res_c, craw_res_f)
                        time.sleep(ratelimit)
                    else:
                        log.info(
                            'Connecting to Discgos API Skipped, too less data')
                        req_res_c, req_res_f = crawl_res_c, craw_res_f
                        req_res_c['d_id'] = ''
                        req_res_c['metoda_did'] = ''
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
            save_to_db(req_res_c, req_res_f)
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
