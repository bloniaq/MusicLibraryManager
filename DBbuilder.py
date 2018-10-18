
from datetime import datetime
import sys
import time
import logging
import signal
import requests

import discogs_con
import config
import dbase
import data_stocker


#############################################
# CONFIGURATION SECTION
#############################################

slasher = config.slasher
ratelimit = config.ratelimit
refresh_catalogs_list = config.refresh_catalogs_list
refresh_database = config.refresh_database
checklist = config.discogs_checklist
update_ids = config.update_ids:

log = logging.getLogger()
log.handlers = []

log = logging.getLogger('main')
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

dbase.create_tables()

if refresh_database:
    dbase.clear_db()


for j in range(STEPS):
    if not refresh_database:
        if dbase.check_if_r_exist(j, cataloglist):
            log.info('Skipped {0} - it exist in DB'.format(j))
            continue
    if not update_ids
        if dbase.check_if_ids_exist(j, cataloglist):
            log.info('Skipped {0} - it contains IDs'.format(j))
            continue
    while True:
        dynamic_ratelimit = 2
        try:
            crawl_res_c, crawl_res_f = data_stocker.catalog_crawler(
                cataloglist[j])
            if (crawl_res_c['artist'] != 'Unknown Artist' and
                    crawl_res_c['album'] != 'Unknown Album'):
                req_res_c = discogs_con.query_discogs(crawl_res_c, crawl_res_f)
                dynamic_ratelimit = 2
                time.sleep(dynamic_ratelimit)
            else:
                log.info(
                    'Connecting to Discgos API Skipped, too less data')
                req_res_c = crawl_res_c
                for i in checklist:
                    cat_attrs.get(i, '')
        except requests.exceptions.ConnectionError as e:
            dynamic_ratelimit = dynamic_ratelimit**2
            if dynamic_ratelimit > 10600:
                interrupted = True
                log.warning(
                'Check your connection. Closing script')
            else:
                log.warning('{}'.format(e))
                log.warning(
                    'Connection Broken. Trying to connect in {0} seconds'.format(
                    dynamic_ratelimit))
            time.sleep(dynamic_ratelimit)
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
