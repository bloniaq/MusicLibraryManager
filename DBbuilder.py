from datetime import datetime
import sys
import time
import logging
import signal
import requests

import discogs_con
import mlm_config
import dbase
import data_stocker


#############################################
# CONFIGURATION SECTION
#############################################

slasher = mlm_config.slasher
ratelimit = mlm_config.ratelimit
refresh_catalogs_list = mlm_config.refresh_catalogs_list
refresh_database = mlm_config.refresh_database

log = logging.getLogger()
log.handlers = []

log = logging.getLogger('DBbuilder')
log.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
fh = logging.FileHandler(datetime.now().strftime(
    'logs' + slasher + 'log-%Y.%m.%d-%H.%M.log'), 'w', 'utf-8')
fh.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    '%(asctime)s-%(name)s-%(levelname)s: %(message)s',
    datefmt='%Y.%m.%d %H:%M:%S')

ch.setFormatter(formatter)
fh.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)


if sys.platform == 'linux':
    log.info('OS : Linux')
if sys.platform == 'win32':
    log.info('OS : Windows')


def signal_handler(signal, frame):
    global interrupted
    interrupted = True


signal.signal(signal.SIGINT, signal_handler)

interrupted = False


#############################################
# MAIN PROGRAM SECTION
#############################################


cataloglist = data_stocker.catalog_collector(refresh_catalogs_list)

STEPS = len(cataloglist)


if refresh_database:
    dbase.clear_db()


for j in range(STEPS):
    if not refresh_database:
        existingrecord = dbase.check_if_r_exist(j, cataloglist)
        if existingrecord is not None:
            log.info('Skipped {0}'.format(existingrecord))
            continue
    while True:
        try:
            crawl_res_c, crawl_res_f = data_stocker.catalog_crawler(
                cataloglist[j])
            if (crawl_res_c['artist'] != 'Unknown Artist' and
                    crawl_res_c['album'] != 'Unknown Album'):
                req_res_c = discogs_con.find_album_d_master(
                    crawl_res_c, crawl_res_f)
                time.sleep(ratelimit)
            else:
                log.info(
                    'Connecting to Discgos API Skipped, too less data')
                req_res_c = crawl_res_c
                req_res_c['d_master'] = ''
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
    dbase.save_to_db(req_res_c, crawl_res_f)
    if interrupted:
        print("Exiting Script")
        j = range(STEPS)[-1]
        break
