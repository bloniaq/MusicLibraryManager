import sys
import signal

if sys.platform == 'linux':
    inputpath = "/home/kuba/Muzyka"
    slasher = '/'
    catalog_cache_file = 'lin_catalogs.dat'
    ratelimit = 2
    databasefilename = 'lin_database.db'
    refresh_catalogs_list = True
    refresh_database = False
if sys.platform == 'win32':
    inputpath = "D:\\++WORKZONE++"
    slasher = '\\'
    catalog_cache_file = 'win_catalogs.dat'
    ratelimit = 1
    databasefilename = 'database.db'
    refresh_catalogs_list = False
    refresh_database = False  # UWAGA

update_ids = False
manual_mode = False
test_run = False

supported_list = ['mp3', 'ogg', 'flac', 'wav', 'wma', 'ape']
img_ext_list = ['jpg', 'jpeg', 'png']
unsupported_list = ['cue']
discogs_checklist = ['d_master', 'd_release',
                     'metoda_master', 'metoda_release']


def signal_handler(signal, frame):
    global interrupted
    interrupted = True


def term_handler(signal, frame):
    global terminated
    terminated = True


def signal_trig():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGBREAK, term_handler)


def is_interrupted():
    flag = False
    if interrupted:
        flag = True
        print("Exiting Script")
    return flag


interrupted = False
terminated = False
