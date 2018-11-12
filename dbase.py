import sqlite3
import logging
from datetime import datetime

import config

databasefilename = config.databasefilename

con = sqlite3.connect(databasefilename)
con.row_factory = sqlite3.Row
cursor = con.cursor()

mod_log = logging.getLogger('main.dbase')

translate_c = {
    'id': 'id',
    'sciezka': 'path',
    'artysta': 'artist',
    'album': 'album',
    'data': 'date',
    'discogs_master': 'd_master',
    'discogs_release': 'd_release',
    'jakosc': 'avg_bitrate',
    'dlugosc': 'total_length',
    'liczba_utworow': 'total_tracks',
    'uwagi': 'comment',
    'format': 'ext',
    'katalog': 'catalog_mark',
    'metoda_master': 'metoda_master',
    'metoda_release': 'metoda_release',
    'obrazki': 'img_count',
    'data_dod': 'insert_date'
}

translate_f = {
    'id': 'id',
    'sciezka': 'path',
    'artysta': 'artist',
    'tytul': 'title',
    'katalog_id': 'catalog_id',
    'album': 'album',
    'format': 'ext',
    'katalog': 'catalog_mark',
    'dlugosc': 'length',
    'jakosc': 'bitrate',
    'data_dod': 'insert_date'
}


def clear_db():
    cursor.executescript("""
        DROP TABLE IF EXISTS katalogi;
        DROP TABLE IF EXISTS pliki;
        """)
    create_tables()


def get_discogs_stats():
    cursor.execute(
        "SELECT id FROM katalogi WHERE id != ?", ('',))
    rows = cursor.fetchall()
    rows = len(rows)
    cursor.execute(
        "SELECT discogs_master FROM katalogi WHERE discogs_master != ?", ('',))
    masters = cursor.fetchall()
    masters = len(masters)
    cursor.execute(
        """SELECT discogs_release FROM
        katalogi WHERE discogs_release != ?""", ('',))
    releases = cursor.fetchall()
    releases = len(releases)
    return rows, masters, releases


def check_if_r_exist(iterator, cat_list):
    flag = False
    cursor.execute(
        'SELECT sciezka FROM katalogi WHERE sciezka = ?',
        (cat_list[iterator],))
    existingrecord = cursor.fetchone()
    if existingrecord is not None:
        flag = True
    return flag


def get_id_by_path(path):
    cursor.execute(
        'SELECT id FROM katalogi WHERE sciezka = ?',
        (path,))
    return cursor.fetchone()[0]


def get_a_dict_by_id(row):
    mod_log.info('get a dict by id {0} started'.format(row))
    cursor.execute(
        'SELECT * FROM katalogi WHERE id = ?',
        (row,))
    dictionary = {}
    result = cursor.fetchall()
    for idx, col in enumerate(cursor.description):
        dictionary[translate_c[col[0]]] = result[0][idx]
    mod_log.info('Generated dictionary : {0}\n\n'.format(dictionary))
    return dictionary


def get_f_dict_by_id(catalog_id):
    file_attr_list = []
    cursor.execute(
        'SELECT * FROM pliki WHERE katalog_id = ?',
        (catalog_id,))
    files = cursor.fetchall()
    for file in files:
        file_attr = {}
        ind = files.index(file)
        for idx, col in enumerate(cursor.description):
            file_attr[translate_f[col[0]]] = files[ind][idx]
        file_attr_list.append(file_attr)
    mod_log.info('Generated list : {0}\n\n'.format(file_attr_list))
    return file_attr_list


def check_if_id_has_value(row, column):
    flag = True
    query = "SELECT {0} FROM katalogi WHERE id = ?".format(column)
    cursor.execute(query, (row,))
    value = cursor.fetchone()
    if not value or value[0] == '':
        flag = False
    return flag


def add_column():
    cursor.executescript("""
        ALTER TABLE katalogi
            ADD data_dod DATE;
        ALTER TABLE pliki
            ADD data_dod DATE;
        """)


def create_tables():
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS katalogi (
            id INTEGER PRIMARY KEY ASC,
            sciezka varchar(400) NOT NULL,
            artysta varchar(250) NOT NULL,
            album varchar(250) NOT NULL,
            data DATE,
            discogs_master varchar(250),
            discogs_release varchar(250),
            jakosc INTEGER NOT NULL,
            dlugosc INTEGER,
            liczba_utworow INTEGER NOT NULL,
            uwagi varchar(1400),
            format varchar(20),
            katalog varchar(200),
            metoda_master varchar(100),
            metoda_release varchar(100),
            obrazki INTEGER,
            data_dod DATE
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
            format varchar(20),
            dlugosc INTEGER,
            jakosc INTEGER NOT NULL,
            data_dod DATE,
            FOREIGN KEY(katalog_id) REFERENCES katalogi(id)
        )""")


def save_to_db(cat_attrs, f_attrs_list):
    mod_log.info('Save to DB started')
    if 'id' in cat_attrs:
        cat_attrs['insert_date'] = datetime.now().strftime('%Y.%m.%d-%H.%M')
        cursor.execute("""
            REPLACE INTO katalogi
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                       (cat_attrs['id'],
                        cat_attrs['path'],
                        cat_attrs['artist'],
                           cat_attrs['album'],
                           cat_attrs['date'],
                           cat_attrs['d_master'],
                           cat_attrs['d_release'],
                           cat_attrs['avg_bitrate'],
                           cat_attrs['total_length'],
                           cat_attrs['total_tracks'],
                           cat_attrs['comment'],
                           cat_attrs['ext'],
                           cat_attrs['catalog_mark'],
                           cat_attrs['metoda_master'],
                           cat_attrs['metoda_release'],
                           cat_attrs['img_count'],
                           cat_attrs['insert_date']))
        mod_log.info('Catalog data inserted successfully')
        for i in range(len(f_attrs_list)):
            f_attrs_list[i]['insert_date'] = datetime.now().strftime(
                '%Y.%m.%d-%H.%M')
            mod_log.debug('Tries to insert : {0}'.format(f_attrs_list[i]))
            cursor.execute(
                'REPLACE INTO pliki VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);',
                (f_attrs_list[i]['id'],
                 f_attrs_list[i]['path'],
                 f_attrs_list[i]['artist'],
                 f_attrs_list[i]['title'],
                 cat_attrs['id'],
                 f_attrs_list[i]['album'],
                 f_attrs_list[i]['ext'],
                 f_attrs_list[i]['length'],
                 f_attrs_list[i]['bitrate'],
                 f_attrs_list[i]['insert_date']
                 ))
        mod_log.info('Files data inserted successfully')
    else:
        cat_attrs['insert_date'] = datetime.now().strftime('%Y.%m.%d-%H.%M')
        cursor.execute("""
            INSERT INTO katalogi
            VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                       (cat_attrs['path'],
                        cat_attrs['artist'],
                           cat_attrs['album'],
                           cat_attrs['date'],
                           cat_attrs['d_master'],
                           cat_attrs['d_release'],
                           cat_attrs['avg_bitrate'],
                           cat_attrs['total_length'],
                           cat_attrs['total_tracks'],
                           cat_attrs['comment'],
                           cat_attrs['ext'],
                           cat_attrs['catalog_mark'],
                           cat_attrs['metoda_master'],
                           cat_attrs['metoda_release'],
                           cat_attrs['img_count'],
                           cat_attrs['insert_date']))
        mod_log.info('Catalog data inserted successfully')
        catalogid = cursor.lastrowid
        for i in range(len(f_attrs_list)):
            mod_log.debug('Tries to insert : {0}'.format(f_attrs_list[i]))
            f_attrs_list[i]['insert_date'] = datetime.now().strftime(
                '%Y.%m.%d-%H.%M')
            cursor.execute(
                'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?);',
                (f_attrs_list[i]['path'],
                 f_attrs_list[i]['artist'],
                 f_attrs_list[i]['title'],
                 catalogid,
                 f_attrs_list[i]['album'],
                 f_attrs_list[i]['ext'],
                 f_attrs_list[i]['length'],
                 f_attrs_list[i]['bitrate'],
                 f_attrs_list[i]['insert_date']
                 ))
        mod_log.info('Files data inserted successfully')
    con.commit()
    mod_log.info('Data Commited\n')
