
from datetime import datetime
import sys
import time
import logging
import signal
import requests
import discogs_client

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
update_ids = config.update_ids

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


def query_discogs(crawl_res_c, crawl_res_f):
    global interrupted
    while True:
        dynamic_ratelimit = 2
        try:
            if (crawl_res_c['artist'] != 'Unknown Artist' and
                    crawl_res_c['album'] != 'Unknown Album'):
                req_res_c = discogs_con.insert_ids(crawl_res_c, crawl_res_f)
                dynamic_ratelimit = 2
                time.sleep(dynamic_ratelimit)
            else:
                log.info(
                    'Connecting to Discgos API Skipped, too less data')
                req_res_c = crawl_res_c
                for i in checklist:
                    req_res_c[i] = ''
        except (
            requests.exceptions.ConnectionError,
                discogs_client.exceptions.HTTPError) as e:
            dynamic_ratelimit = dynamic_ratelimit**2
            if dynamic_ratelimit > 10600:
                interrupted = True
                log.warning(
                    'Check your connection. Closing script {0}'.format(e))
            else:
                log.warning('{}'.format(e))
                log.warning(
                    'Connection Broken. \
                    Trying to connect in {0} seconds'.format(
                        dynamic_ratelimit))
            time.sleep(dynamic_ratelimit)
            if interrupted:
                print("Exiting Script")
                break
            continue
        break
    return req_res_c


#############################################
# MAIN PROGRAM SECTION
#############################################


cataloglist = data_stocker.catalog_collector(refresh_catalogs_list)

STEPS = len(cataloglist)

dbase.create_tables()

in_masters, in_releases = dbase.get_discogs_stats()
log.info('There is {0} masters, and {1} releases recognizes in db'.format(
    in_masters, in_releases))

if refresh_database:
    dbase.clear_db()

for j in range(STEPS):
    if not update_ids and dbase.check_if_r_exist(
            j, cataloglist):
        log.info('Skipped {0} - it exist in DB'.format(j))
        continue
    if update_ids and dbase.check_if_r_exist(j, cataloglist):
        current_id = dbase.get_id_by_path(cataloglist[j])
        if (dbase.check_if_id_has_value(current_id, 'discogs_master') and
                dbase.check_if_id_has_value(current_id, 'discogs_release')):
            log.info('Skipped {0} - it contains both IDs'.format(j))
            continue
        else:
            crawl_res_c = dbase.get_a_dict_by_id(current_id)
            crawl_res_f = dbase.get_f_dict_by_id(current_id)
    if not dbase.check_if_r_exist(j, cataloglist):
        crawl_res_c, crawl_res_f = data_stocker.catalog_crawler(
            cataloglist[j])
    req_res_c = query_discogs(crawl_res_c, crawl_res_f)
    dbase.save_to_db(req_res_c, crawl_res_f)
    if interrupted:
        break

print("Exiting Script")
out_masters, out_releases = dbase.get_discogs_stats()
log.info('There is {0} masters, and {1} releases recognizes in db'.format(
    out_masters, out_releases))
log.info('In this session found {0} masters and {1} releases'.format(
    out_masters - in_masters, out_releases - in_releases))
