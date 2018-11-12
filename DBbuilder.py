from datetime import datetime
import sys
import time
import logging

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

config.signal_trig()


def query_discogs(crawl_res_c, crawl_res_f):
    dynamic_ratelimit = 2
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
    return req_res_c


def test_run():
    log.info('##########################################')
    log.info('')
    log.info('\t\tTEST RUN')
    log.info('')
    log.info('##########################################')
    test_id_list = [3789]
    for i in test_id_list:
        dictionary = dbase.get_a_dict_by_id(i)
        log.info('Dict: {0}\n'.format(dictionary))
        test_result = discogs_con.m_by_album_fuzz(dictionary)
        log.info('Result: {0}'.format(test_result))
    config.interrupted = True


#############################################
# MAIN PROGRAM SECTION
#############################################


cataloglist = data_stocker.catalog_collector(refresh_catalogs_list)

STEPS = len(cataloglist)

dbase.create_tables()

in_rows, in_masters, in_releases = dbase.get_discogs_stats()
log.info('There is {0} masters, and {1} releases recognizes in db'.format(
    in_masters, in_releases))

if config.test_run:
    test_run()

if refresh_database:
    dbase.clear_db()

for j in range(STEPS):
    if config.interrupted:
        print("Exiting Script")
        break
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
    if config.interrupted:
        print("Finding discogs ID interrupted")
        break
    dbase.save_to_db(req_res_c, crawl_res_f)
    if config.interrupted or config.terminated:
        break

print("Exiting Script")
out_rows, out_masters, out_releases = dbase.get_discogs_stats()
log.info('There is {2} rows at all, {0} masters, and {1} \
releases recognizes in db'.format(out_masters, out_releases, out_rows))
log.info('In this session added {2} rows, found {0} masters and {1} \
releases'.format(out_masters - in_masters, out_releases - in_releases,
                 out_rows - in_rows))
