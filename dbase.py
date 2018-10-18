import sqlite3
import logging

import config

databasefilename = config.databasefilename

con = sqlite3.connect(databasefilename)
con.row_factory = sqlite3.Row
cursor = con.cursor()

mod_log = logging.getLogger('main.dbase')


def clear_db():
    cursor.executescript("""
        DROP TABLE IF EXISTS katalogi;
        DROP TABLE IF EXISTS pliki;
        """)
    create_tables()


def check_if_r_exist(iterator, cat_list):
    cursor.execute(
        'SELECT sciezka FROM katalogi WHERE sciezka = ?',
        (cat_list[iterator],))
    existingrecord = cursor.fetchone()
    return existingrecord


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
            uwagi varchar(400),
            format varchar(20),
            katalog varchar(200),
            metoda_master varchar(100),
            metoda_release varchar(100),
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
            format varchar(20),
            dlugosc INTEGER,
            jakosc INTEGER NOT NULL,
            FOREIGN KEY(katalog_id) REFERENCES katalogi(id)
        )""")


def save_to_db(cat_attrs, f_attrs_list):
    mod_log.info('Save to DB started')
    cursor.execute("""
        INSERT INTO katalogi
        VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""",
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
         cat_attrs['img_count']))
    mod_log.info('Catalog data inserted successfully')
    catalogid = cursor.lastrowid
    for i in range(len(f_attrs_list)):
        mod_log.debug('Tries to insert : {0}'.format(f_attrs_list[i]))
        cursor.execute(
            'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?);',
            (f_attrs_list[i]['path'],
             f_attrs_list[i]['artist'],
             f_attrs_list[i]['title'],
             catalogid,
             f_attrs_list[i]['album'],
             f_attrs_list[i]['ext'],
             f_attrs_list[i]['length'],
             f_attrs_list[i]['bitrate']
             ))
    mod_log.info('Catalog data inserted successfully')
    con.commit()
    mod_log.info('Data Commited\n')
