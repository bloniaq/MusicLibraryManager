import sys

if sys.platform == 'linux':
    inputpath = "/home/kuba/Muzyka"
    slasher = '/'
    catalog_cache_file = 'lin_catalogs.dat'
    ratelimit = 2
    databasefilename = 'lin_database.db'
    refresh_catalogs_list = True
    refresh_database = True
if sys.platform == 'win32':
    inputpath = "D:\\++WORKZONE++"
    slasher = '\\'
    catalog_cache_file = 'win_catalogs.dat'
    ratelimit = 1
    databasefilename = 'database.db'
    refresh_catalogs_list = False
    refresh_database = False

supported_list = ['mp3', 'ogg', 'flac', 'wav', 'wma', 'ape']
img_ext_list = ['jpg', 'jpeg', 'png']
unsupported_list = ['cue']