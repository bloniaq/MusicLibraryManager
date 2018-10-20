from datetime import timedelta
import os
import logging
import taglib
import ntpath
import pickle

import config
import text_tools

inputpath = config.inputpath
catalog_cache_file = config.catalog_cache_file
refresh = config.refresh_catalogs_list
supported_list = config.supported_list
unsupported_list = config.unsupported_list
img_ext_list = config.img_ext_list
slasher = config.slasher
ratelimit = config.ratelimit


log = logging.getLogger('main.stocker')


def catalog_collector(refresh, path=inputpath):
    '''Loads or creates a list od every catalog in path containg audio file
    '''
    result = []
    log.info('catalog_collector Function starts')
    if os.path.exists(catalog_cache_file) and refresh is False:
        log.debug('Catalog Cache File founded')
        with open(catalog_cache_file, 'rb') as file:
            result = pickle.load(file)
    else:
        log.debug('Catalog Cache File not found')
        log.debug('Starting reading library')
        xcounter = 0
        for x in os.walk(path):
            xcounter += 1
            for i in os.listdir(x[0]):
                if extension_check(i, supported_list):
                    result.append(x[0])
                    break
            if xcounter % 500 == 0:
                log.debug('Already checked {0} directories'.format(xcounter))
        log.info('Library reading ended with {0} directories'.format(xcounter))
        with open(catalog_cache_file, 'wb') as file2:
            pickle.dump(result, file2)
    log.info(
        'catalog_collector found {0} dirs to check\n\n'.format(len(result)))
    return result


'''
dirs = []
for _, x, _ in os.walk(path):
dirs[:] = [di for di in x if di not in ['$RECYCLE.BIN']]
xcounter += 1
log.info(dirs)
for j in dirs:
    for i in os.listdir(os.path.normpath(x[0])):
        if extension_check(i, supported_list):
            result.append(x)
            break
'''


def get_extension(file):
    filename, tail = os.path.splitext(file)
    extension = tail[1:]
    return extension


def extension_check(file, extension_list):
    '''checks if input file has extension contained on extensions list
    '''
    extension = get_extension(file)
    if extension in extension_list:
        return True
    else:
        return False


#############################################
# CATALOG OPERATIONS FUNCTIONS DEFINITIONS
#############################################


def catalog_crawler(path):
    '''returns data got from audiofiles tags'''
    files_attributes_list = []
    image_count = 0
    supported_flag = True
    log.info('Catalog Crawler starts')
    files = [f for f in os.listdir(path)
             if (os.path.isfile(os.path.join(path, f)))]
    log.debug('Files found in catalog : {0}'.format(files))
    log.info('Starting loop through files\n')
    for i in files:
        if extension_check(i, supported_list):
            files_attributes_list.append(
                file_crawler(path + slasher + i))
        if extension_check(i, unsupported_list):
            supported_flag = False
    log.info('\n')
    log.info('Loop through files ends succesfully\n')
    catalog_attributes = determine_cat_attr(files_attributes_list)
    for (dirp, dirn, fnam) in os.walk(path):
        for j in fnam:
            if extension_check(j, img_ext_list):
                image_count += 1
    catalog_attributes['img_count'] = image_count
    catalog_attributes['path'] = path
    catalog_attributes['comment'] = ''
    if not supported_flag:
        catalog_attributes['comment'] = 'unsupported format'
    log.debug('returns {0}'.format(catalog_attributes))
    log.info('Catalog Crawler ends,\n')
    return catalog_attributes, files_attributes_list


def file_crawler(filepath):
    '''gets data from audiofile tags and returns it in dictionary'''
    log.info('File Crawler for starts for file : {0}'.format(filepath))
    file_attributes = {}
    file_attributes['path'] = filepath
    file_attributes['ext'] = get_extension(ntpath.basename(filepath))
    file_attributes['album'] = ''
    file_attributes['artist'] = ''
    file_attributes['title'] = ''
    file_attributes['date'] = ''
    file_attributes['length'] = 0
    file_attributes['bitrate'] = 0
    try:
        audiofile = taglib.File(filepath)
    except OSError as e:
        log.warning('OS Error : {0}'.format(e))
    else:
        log.debug('Handling file by taglib - correct')
    try:
        try:
            file_attributes['album'] = audiofile.tags['ALBUM'][0]
        except (KeyError, IndexError) as e:
            log.warning('Error : {0}'.format(e))
        try:
            file_attributes['artist'] = audiofile.tags['ARTIST'][0]
        except (KeyError, IndexError) as e:
            log.warning('Error : {0}'.format(e))
        try:
            file_attributes['title'] = audiofile.tags['TITLE'][0]
        except (KeyError, IndexError) as e:
            log.warning('Error : {0}'.format(e))
        try:
            file_attributes['date'] = audiofile.tags['DATE'][0]
        except (KeyError, IndexError) as e:
            log.warning('Error : {0}'.format(e))
        file_attributes['length'] = audiofile.length
        file_attributes['bitrate'] = audiofile.bitrate
    except BaseException as e:
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
        if (i['album'] != '' and
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
        if (i['artist'] != '' and
                i['artist'] not in cat_artist_tags):
            cat_artist_tags.append(i['artist'])
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
        cat_attr['artist'] = text_tools.handle_multiple_artist(cat_artist_tags)
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
        except KeyError:
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
    cat_attr['avg_bitrate'] = round((bitrate_sum / len(f_attr_list)), 1)

    # Total Length

    log.debug('Computing total length of files')
    tot_len = 0
    for i in f_attr_list:
        tot_len += i['length']
    cat_attr['total_length'] = tot_len

    # Catalog Symbol -- IN FUTURE

    cat_attr['catalog_mark'] = ''

    # Extension
    files_extensions = []
    for i in f_attr_list:
        if i['ext'] not in files_extensions:
            files_extensions.append(i['ext'])
    if len(files_extensions) == 1:
        cat_attr['ext'] = files_extensions[0]
    else:
        cat_attr['ext'] = 'various'

    # Total Tracks

    cat_attr['total_tracks'] = len(f_attr_list)

    # Summary

    log.info('reckognized Catalog Attributes:\nArtist: {0}\
        \nAlbum: {1}\nDate: {2}\nAvgBitrate: {3}\nCatalogSym{4}\
        \nTotal Tracks: {6}\nTotal Length: {5}\n\n'.format(
        cat_attr['artist'], cat_attr['album'], cat_attr['date'],
        cat_attr['avg_bitrate'], cat_attr['catalog_mark'],
        str(timedelta(seconds=cat_attr['total_length'])),
        cat_attr['total_tracks']))
    return cat_attr
