# This script automatically extracts a wide selection of statistics from www.sports-reference.com
# for teams competing in the NCAA men's basketball tournament since 2010, and saves the information
# in a six-table SQLite relational database.

import os
import re
import time
import sqlite3
import urllib.request, urllib.parse, urllib.error

os.chdir('C:/Users/Michael/Dropbox/Documents/Python/march madness')
conn = sqlite3.connect('mm_web_dump.sqlite')
cur = conn.cursor()
cur.executescript('''
DROP TABLE IF EXISTS School;
DROP TABLE IF EXISTS Season;
DROP TABLE IF EXISTS Team_Stats1;
DROP TABLE IF EXISTS Team_Stats2;
DROP TABLE IF EXISTS Opp_Stats1;
DROP TABLE IF EXISTS Opp_Stats2;

CREATE TABLE School (
    id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name  TEXT UNIQUE
);

CREATE TABLE Season (
    id    INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    year  INTEGER UNIQUE
);

CREATE TABLE Team_Stats1 (
    school_id    INTEGER,
    season_id    INTEGER,
    minutes      INTEGER,
    srs          FLOAT,
    pts          INTEGER,
    fg3          INTEGER,
    ft           INTEGER,
    off_reb      INTEGER,
    def_reb      INTEGER,
    ast          INTEGER,
    stl          INTEGER,
    blk          INTEGER,
    tov          INTEGER,
    PRIMARY KEY (school_id, season_id)
);

CREATE TABLE Opp_Stats1 (
    school_id    INTEGER,
    season_id    INTEGER,
    opp_pts      INTEGER,
    opp_fg3      INTEGER,
    opp_fta      INTEGER,
    opp_off_reb  INTEGER,
    opp_def_reb  INTEGER,
    opp_ast      INTEGER,
    opp_stl      INTEGER,
    opp_blk      INTEGER,
    opp_tov      INTEGER,
    PRIMARY KEY (school_id, season_id)
);

CREATE TABLE Team_Stats2 (
    school_id    INTEGER,
    season_id    INTEGER,
    pace         FLOAT,
    ast_pct      FLOAT,
    blk_pct      FLOAT,
    tov_pct      FLOAT,
    PRIMARY KEY (school_id, season_id)
);

CREATE TABLE Opp_Stats2 (
    school_id    INTEGER,
    season_id    INTEGER,
    opp_blk_pct  FLOAT,
    opp_tov_pct  FLOAT,
    PRIMARY KEY (school_id, season_id)
)
''')

# this dictionary fixes school names that are mis-matched between the statistics table URLs and the bracket results URLs
school_dict = {'Ole Miss':'Mississippi', 'USC':'Southern California', 'California':'University of California', 'UNC':'North Carolina',
               'UNC Greensboro':'North Carolina-Greensboro', "St. Joseph's":"Saint Joseph's", 'BYU':'Brigham Young',
               'NC State':'North Carolina State', 'UConn':'Connecticut', 'UNC Wilmington':'North Carolina-Wilmington',
               'Penn':'Pennsylvania', 'Pitt':'Pittsburgh', 'Central Connecticut':'Central Connecticut State', 'UCSB':'UC-Santa Barbara',
               'UIC':'Illinois-Chicago', 'UNC Asheville':'North Carolina-Asheville', 'LSU':'Louisiana State', 'ETSU':'East Tennessee State',
               'VCU':'Virginia Commonwealth', 'UCF':'Central Florida', 'UTSA':'Texas-San Antonio', 'UTEP':'Texas-El Paso',
               "Saint Mary's":"Saint Mary's (CA)", 'UNLV':'Nevada-Las Vegas', 'Long Beach State':'Cal State Long Beach',
               'UMBC':'Maryland-Baltimore County', 'LIU':'Long Island University', "St. Peter's":"Saint Peter's", 'Detroit':'Detroit Mercy',
               'Southern Miss':'Southern Mississippi', 'UMass':'Massachusetts', 'SMU':'Southern Methodist', 'TCU':'Texas Christian'}

end_year   = int(input('Starting year = 2010. Please enter ending year...'))

for year in list(range(2010, end_year+1)):
    ###### CYCLE THROUGH SEASONS ######
    print(year)
    cur.execute('INSERT OR IGNORE INTO Season (year) VALUES ( ? )', ( year, ) )
    cur.execute('SELECT id FROM Season WHERE year = ? ', (year, ))
    season_id = cur.fetchone()[0]

    ###### BASIC TEAM STATISTICS ######
    print('...basic team stats')
    time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-school-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=\S*>(.*)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        minutes = int(  re.findall('data-stat="mp" >(\S*)</td>', linetext)[0])
        srs     = float(re.findall('data-stat="srs" >(\S*)</td>', linetext)[0])
        pts     = int(  re.findall('data-stat="pts" >(\S*)</td>', linetext)[0])
        fg3     = int(  re.findall('data-stat="fg3" >(\S*)</td>', linetext)[0])
        ft      = float(re.findall('data-stat="ft" >(\S*)</td>', linetext)[0])
        # try:                  # offensive rebounds are not recorded for some schools in earlier seasons
        off_reb = int(  re.findall('data-stat="orb" >(\S*)</td>', linetext)[0])
        # except:               # will need to impute missing values prior to predictive modeling
            # off_reb = 0
        def_reb = int(  re.findall('data-stat="trb" >(\S*)</td>', linetext)[0]) - off_reb
        ast     = int(  re.findall('data-stat="ast" >(\S*)</td>', linetext)[0])
        stl     = int(  re.findall('data-stat="stl" >(\S*)</td>', linetext)[0])
        blk     = int(  re.findall('data-stat="blk" >(\S*)</td>', linetext)[0])
        tov     = int(  re.findall('data-stat="tov" >(\S*)</td>', linetext)[0])
        
        cur.execute('INSERT OR IGNORE INTO School (name) VALUES ( ? )', ( school, ) )
        cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
        school_id = cur.fetchone()[0]
        cur.execute('''INSERT OR REPLACE INTO Team_Stats1
        (school_id, season_id, minutes, srs, pts, fg3, ft, off_reb, def_reb, ast, stl, blk, tov) 
         VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
        (school_id, season_id, minutes, srs, pts, fg3, ft, off_reb, def_reb, ast, stl, blk, tov) )
    conn.commit()

    ###### BASIC OPPONENT STATISTICS ######
    print('...basic opponent stats')
    time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-opponent-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=\S*>(.*)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        opp_pts     = int(  re.findall('data-stat="opp_pts" >(\S*)</td>', linetext)[0])
        opp_fg3     = int(  re.findall('data-stat="opp_fg3" >(\S*)</td>', linetext)[0])
        opp_fta     = float(re.findall('data-stat="opp_fta" >(\S*)</td>', linetext)[0])
        opp_off_reb = int(  re.findall('data-stat="opp_orb" >(\S*)</td>', linetext)[0])
        opp_def_reb = int(  re.findall('data-stat="opp_trb" >(\S*)</td>', linetext)[0]) - opp_off_reb
        opp_ast     = int(  re.findall('data-stat="opp_ast" >(\S*)</td>', linetext)[0])
        opp_stl     = int(  re.findall('data-stat="opp_stl" >(\S*)</td>', linetext)[0])
        opp_blk     = int(  re.findall('data-stat="opp_blk" >(\S*)</td>', linetext)[0])
        opp_tov     = int(  re.findall('data-stat="opp_tov" >(\S*)</td>', linetext)[0])
        
        # cur.execute('INSERT OR IGNORE INTO School (name) VALUES ( ? )', ( school, ) )
        cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
        school_id = cur.fetchone()[0]
        cur.execute('''INSERT OR REPLACE INTO Opp_Stats1
        (school_id, season_id, opp_pts, opp_fg3, opp_fta, opp_off_reb, opp_def_reb, opp_ast, opp_stl, opp_blk, opp_tov) 
         VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
        (school_id, season_id, opp_pts, opp_fg3, opp_fta, opp_off_reb, opp_def_reb, opp_ast, opp_stl, opp_blk, opp_tov) )
    conn.commit()

    ###### ADVANCED TEAM STATISTICS ######
    print('...advanced team stats')
    time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-advanced-school-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=\S*>(.*)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        pace    = float(re.findall('data-stat="pace" >(\S*)</td>', linetext)[0])
        ast_pct = float(re.findall('data-stat="ast_pct" >(\S*)</td>', linetext)[0])
        blk_pct = float(re.findall('data-stat="blk_pct" >(\S*)</td>', linetext)[0])
        tov_pct = float(re.findall('data-stat="tov_pct" >(\S*)</td>', linetext)[0])

        # cur.execute('INSERT OR IGNORE INTO School (name) VALUES ( ? )', ( school, ) )
        cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
        school_id = cur.fetchone()[0]
        cur.execute('''INSERT OR REPLACE INTO Team_Stats2
        (school_id, season_id, pace, ast_pct, blk_pct, tov_pct) 
         VALUES ( ?, ?, ?, ?, ?, ? )''',
        (school_id, season_id, pace, ast_pct, blk_pct, tov_pct) )
    conn.commit()

    ###### ADVANCED OPPONENT STATISTICS ######
    print('...advanced opponent stats')
    time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-advanced-opponent-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=\S*>(.*)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        opp_blk_pct = float(re.findall('data-stat="opp_blk_pct" >(\S*)</td>', linetext)[0])
        opp_tov_pct = float(re.findall('data-stat="opp_tov_pct" >(\S*)</td>', linetext)[0])

        # cur.execute('INSERT OR IGNORE INTO School (name) VALUES ( ? )', ( school, ) )
        cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
        school_id = cur.fetchone()[0]
        cur.execute('''INSERT OR REPLACE INTO Opp_Stats2
        (school_id, season_id, opp_blk_pct, opp_tov_pct) 
         VALUES ( ?, ?, ?, ? )''',
        (school_id, season_id, opp_blk_pct, opp_tov_pct) )
    conn.commit()

print('Data scraping successfully completed.')
