import sqlite3
import os
import taglib
import discogs_client
import itertools
from difflib import SequenceMatcher
import string

inputpath = "/home/kuba/Muzyka"
extlist = ['.mp3', '.ogg', '.flac', '.wav', '.wma', '.ape']

con = sqlite3.connect('database.db')
con.row_factory = sqlite3.Row
cursor = con.cursor()

d = discogs_client.Client(
    'bloniaqsMusicLibraryManager/0.1',
    user_token="BxpsPOkQpsQzPnUErhoQchKfkTIhGxdnzAHhyybD")

punctuationremover = str.maketrans('', '', string.punctuation)


cursor.executescript("""
    DROP TABLE IF EXISTS katalogi;
    CREATE TABLE IF NOT EXISTS katalogi (
        id INTEGER PRIMARY KEY ASC,
        sciezka varchar(400) NOT NULL,
        artysta varchar(250) NOT NULL,
        album varchar(250) NOT NULL,
        discogs varchar(250) DEFAULT ''
    )
    """)

cursor.executescript("""
    DROP TABLE IF EXISTS pliki;
    CREATE TABLE IF NOT EXISTS pliki (
        id INTEGER PRIMARY KEY ASC,
        sciezka varchar(400) NOT NULL,
        artysta varchar(250) DEFAULT '',
        tytul varchar(400) DEFAULT '',
        album varchar(250) DEFAULT '',
        katalog_id INTEGER NOT NULL,
        FOREIGN KEY(katalog_id) REFERENCES katalogi(id)
    )""")


def longestSubstring(str1, str2):
    seqMatch = SequenceMatcher(None, str1, str2)
    match = seqMatch.find_longest_match(0, len(str1), 0, len(str2))
    if match.size != 0:
        # print(str1[match.a: match.a + match.size])
        return str1[match.a: match.a + match.size]
    else:
        print('Nie znaleziono części wspolnych')


def substringFinder(alist):
    comparisionlist = []
    propositions = []
    # print(alist)
    for i in range(len(alist) - 1):
        match = longestSubstring(alist[i], alist[i + 1])
        # print(
        #     'para ', alist[i], ' i ', alist[i + 1], ' : ', match)
        if match not in propositions:
            propositions.append(match)
    print('Propozycje : ', propositions)
    for j in propositions:
        flag = 0
        for k in alist:
            if longestSubstring(j, k) != j:
                flag = 1
        if flag == 0:
            comparisionlist.append(j)
    if len(comparisionlist) == 1:
        return comparisionlist[0]
    else:
        return 'Various Artist'


def AudioFile(file):
    filename, extension = os.path.splitext(file)
    if extension in extlist:
        return True
    else:
        return False


def DiscogsID(artist, album, path):
    print('\n\n***Łącze się z Discogs dla ', artist, ' - ', album)
    token = artist + ' - ' + album
    result = ''
    # ile wyników z zapytania brać pod uwagę
    res_tresh = 50
    try:
        master_album = d.search(album, type='master')
        for i in itertools.islice(master_album, 0, res_tresh):
            masterlist = i.title.split(' - ')
            martist = masterlist[0]
            malbum = masterlist[1]
            if martist == artist and malbum == album:
                print('Znaleziono ID z listy Album')
                print(i.id)
                result = i.id
                break
        i = 0
        if result == '':
            master_token = d.search(token, type='master')
            for i in itertools.islice(master_token, 0, res_tresh):
                print(i.title)
                masterlist = i.title.split(' - ')
                martist = masterlist[0]
                malbum = masterlist[1]
                if martist == artist and malbum == album:
                    print('Znaleziono ID z listy Token')
                    print(i.id)
                    result = i.id
                    break
        i = 0
        if result == '':
            print('\nmetoda Variations')
            varartist = d.search(artist, type='artist')
            variations = varartist[0].name_variations
            print(artist[0], ' aliases : ', variations)
            if variations is not None:
                for k in variations:
                    if result != '':
                        break
                    tokenvar = k + ' - ' + album
                    master_variations = d.search(tokenvar, type='master')
                    for i in itertools.islice(master_variations, 0, res_tresh):
                        masterlist = i.title.split(' - ')
                        martist = masterlist[0].split('*')[0]
                        malbum = masterlist[1]
                        print(martist, ' - ', malbum)
                        if martist == k and malbum == album:
                            print('Znaleziono ID z listy Variations')
                            print(i.id)
                            result = i.id
                            break
        i = 0
        if result == '':
            print('\nmetoda Release')
            release = d.search(album, type='release')
            for i in itertools.islice(release, 0, res_tresh):
                masterlist = i.title.split(' - ')
                martist = masterlist[0]
                malbum = masterlist[1]
                if martist == artist and malbum == album:
                    print('Znaleziono ID z listy Release')
                    print(i.id)
                    result = i.id
                    break
        if result == '':
            print('\nmetoda Release2')
            token_improved = token
            release2 = d.search(token_improved, type='release')
            for i in itertools.islice(release2, 0, res_tresh):
                masterlist = i.title.split(' - ')
                martist = masterlist[0].translate(punctuationremover)
                malbum = masterlist[1].translate(punctuationremover)
                print('elo')
                tranartist = artist.translate(punctuationremover)
                tranalbum = album.translate(punctuationremover)
                if martist == tranartist and malbum == tranalbum:
                    print('Znaleziono ID z listy Release2')
                    print(i.id)
                    result = i.id
                    break
    except IndexError:
        print('przewano szukanie ID')

    return result


def Crawler(path=inputpath):
    '''Lists every catalog path in path which contents at least one audio file
    '''
    result = []
    for x in os.walk(path):
        for i in os.listdir(x[0]):
            if AudioFile(i):
                result.append(x[0])
                break
    return result


def RecognizeCatalog(albumlist, artistlist, path):
    result = {}
    if len(albumlist) == 1:
        result['album'] = albumlist[0]
    else:
        result['album'] = 'Unknown Album'
    if len(artistlist) == 1:
        result['artist'] = artistlist[0]
    elif len(albumlist) == 1:
        result['artist'] = substringFinder(artistlist)
    else:
        result['artist'] = 'Unknown Artist'
    return result


def CatalogWorker(path):
    # print('w tej funkcji maja sie znaleźć działania nad katalogiem')
    albumlist = []
    artistlist = []
    filesidlist = []
    for i in os.listdir(path):
        if AudioFile(i):
            fileid = FileWorker(path + '/' + i, albumlist, artistlist)
            filesidlist.append(fileid)
    token = RecognizeCatalog(albumlist, artistlist, path)
    if (token['artist'] != 'Unknown Artist' and
            token['album'] != 'Unknown Album'):
        discogsid = DiscogsID(token['artist'], token['album'], path)
    else:
        print('\n\nNie rozpoznano albumu')
        discogsid = ''
    cursor.execute(
        'INSERT INTO katalogi VALUES(NULL, ?, ?, ?, ?);',
        (path, token['artist'], token['album'], discogsid))
    catalogid = cursor.lastrowid
    for j in filesidlist:
        cursor.execute(
            'UPDATE pliki SET katalog_id=? WHERE id=?', (catalogid, j))
    con.commit()
    print(
        'CatalogID: ', catalogid, '\nArtysta: ', token['artist'], '\nAlbum: ',
        token['album'], '\n\n')
    # jeśli są jest jedna propozycja - wyszukaj ją na discogs
    # utwórz rekord w db encja Katalogi


def FileWorker(path, albumlist, artistlist):
    # print('w tej funkcji maja byc wykonywane dzialania nad plikiem')
    song = taglib.File(path)
    try:
        albumtag = song.tags['ALBUM'][0]
        if albumtag not in albumlist:
            albumlist.append(albumtag)
    except KeyError as ALBUM:
        print('\nplik ', path)
        print('pole ALBUM błędne lub puste')
        albumtag = ''
    try:
        artisttag = song.tags['ARTIST'][0]
        if artisttag not in artistlist:
            artistlist.append(artisttag)
    except KeyError as ARTIST:
        print('\nplik ', path)
        print('pole artist błędne lub puste')
        artisttag = ''
    try:
        titletag = song.tags['TITLE'][0]
    except KeyError as TITLE:
        print('\nplik ', path)
        print('pole title błędne lub puste')
        titletag = ''
    cursor.execute(
        'INSERT INTO pliki VALUES(NULL, ?, ?, ?, ?, ?);',
        (path, artisttag, titletag, albumtag, 0))
    fileid = cursor.lastrowid
    con.commit()
    return fileid
    # utwórz rekord w db encja Pliki


cataloglist = Crawler()

STEPS = 300

try:
    for j in range(STEPS):
        CatalogWorker(cataloglist[j])
except IndexError:
    print('koniec')
