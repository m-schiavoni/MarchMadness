# This script automatically extracts a wide selection of statistics and tournament results
# from www.sports-reference.com for teams competing in the NCAA men's basketball tournament
# since 2010, and saves the information in an eight-table SQLite relational database.

import os
import re
import time
import sqlite3
import urllib.request

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
DROP TABLE IF EXISTS Rank;
DROP TABLE IF EXISTS Tournament;

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
);

CREATE TABLE Rank (
    school_id    INTEGER,
    season_id    INTEGER,
    tourn_rank   INTEGER,
    PRIMARY KEY (school_id, season_id)
);

CREATE TABLE Tournament (
    season_id    INTEGER,
    school_id1   INTEGER,
    school_id2   INTEGER,
    score1       INTEGER,
    score2       INTEGER,
    PRIMARY KEY (season_id, school_id1, school_id2)
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

# hard-coded to begin in 2010, because limited statistics are available for earlier seasons
end_year = int(input('Starting year = 2010.  Please enter ending year...'))
years = list(range(2010, end_year+1));  years.reverse()
for year in years:
    ###### CYCLE THROUGH SEASONS ######
    print(year)
    if (year==2020):
        print('...no tournament (COVID-19)')
        continue
    cur.execute('INSERT OR IGNORE INTO Season (year) VALUES ( ? )', ( year, ) )
    cur.execute('SELECT id FROM Season WHERE year = ? ', (year, ))
    season_id = cur.fetchone()[0]

    ###### BASIC TEAM STATISTICS ######
    print('...basic team stats')
    # time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-school-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=.+?>(.+?)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        minutes = int(  re.findall('data-stat="mp" >(.+?)</td>', linetext)[0])              # minutes played
        srs     = float(re.findall('data-stat="srs" >(.+?)</td>', linetext)[0])             # Simple Rating System
        pts     = int(  re.findall('data-stat="pts" >(.+?)</td>', linetext)[0])             # points scored
        fg3     = int(  re.findall('data-stat="fg3" >(.+?)</td>', linetext)[0])             # 3-pt field goals
        ft      = float(re.findall('data-stat="ft" >(.+?)</td>', linetext)[0])              # freethrows made
        off_reb = int(  re.findall('data-stat="orb" >(.+?)</td>', linetext)[0])             # offensive rebounds
        def_reb = int(  re.findall('data-stat="trb" >(.+?)</td>', linetext)[0]) - off_reb   # defensive rebounds
        ast     = int(  re.findall('data-stat="ast" >(.+?)</td>', linetext)[0])             # assists
        stl     = int(  re.findall('data-stat="stl" >(.+?)</td>', linetext)[0])             # steals
        blk     = int(  re.findall('data-stat="blk" >(.+?)</td>', linetext)[0])             # blocks
        tov     = int(  re.findall('data-stat="tov" >(.+?)</td>', linetext)[0])             # turnovers
        
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
    # time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-opponent-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=.+?>(.+?)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        opp_pts     = int(  re.findall('data-stat="opp_pts" >(.+?)</td>', linetext)[0])                 # opponent points scored
        opp_fg3     = int(  re.findall('data-stat="opp_fg3" >(.+?)</td>', linetext)[0])                 # opponent 3-pt field goals
        opp_fta     = float(re.findall('data-stat="opp_fta" >(.+?)</td>', linetext)[0])                 # opponent freethrow attempts
        opp_off_reb = int(  re.findall('data-stat="opp_orb" >(.+?)</td>', linetext)[0])                 # opponent offensive rebounds
        opp_def_reb = int(  re.findall('data-stat="opp_trb" >(.+?)</td>', linetext)[0]) - opp_off_reb   # opponent defensive rebounds
        opp_ast     = int(  re.findall('data-stat="opp_ast" >(.+?)</td>', linetext)[0])                 # opponent assists
        opp_stl     = int(  re.findall('data-stat="opp_stl" >(.+?)</td>', linetext)[0])                 # opponent steals
        opp_blk     = int(  re.findall('data-stat="opp_blk" >(.+?)</td>', linetext)[0])                 # opponent blocks
        opp_tov     = int(  re.findall('data-stat="opp_tov" >(.+?)</td>', linetext)[0])                 # opponent turnovers
        
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
    # time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-advanced-school-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=.+?>(.+?)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        pace    = float(re.findall('data-stat="pace" >(.+?)</td>', linetext)[0])        # possessions per 40 minutes
        ast_pct = float(re.findall('data-stat="ast_pct" >(.+?)</td>', linetext)[0])     # percentage of field goals with an assist
        blk_pct = float(re.findall('data-stat="blk_pct" >(.+?)</td>', linetext)[0])     # percentage of opponent 2-pt fg attempts blocked
        tov_pct = float(re.findall('data-stat="tov_pct" >(.+?)</td>', linetext)[0])     # percentage of plays resulting in a turnover

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
    # time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/seasons/' + str(year) + '-advanced-opponent-stats.html')
    for line in fhand:
        linetext = line.decode().strip()
        if '<small>NCAA</small>' not in linetext:  # skip schools that didn't appear in the tournament
            continue
        school = re.findall('data-stat="school_name" ><a href=.+?>(.+?)</a>', linetext)[0]
        if ('&amp;' in school):  school = school.replace('&amp;', '&')
        opp_blk_pct = float(re.findall('data-stat="opp_blk_pct" >(.+?)</td>', linetext)[0])     # percentage of 2-pt fg attempts blocked by opponent
        opp_tov_pct = float(re.findall('data-stat="opp_tov_pct" >(.+?)</td>', linetext)[0])     # percentage opponent plays resulting in a turnover

        # cur.execute('INSERT OR IGNORE INTO School (name) VALUES ( ? )', ( school, ) )
        cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
        school_id = cur.fetchone()[0]
        cur.execute('''INSERT OR REPLACE INTO Opp_Stats2
        (school_id, season_id, opp_blk_pct, opp_tov_pct) 
         VALUES ( ?, ?, ?, ? )''',
        (school_id, season_id, opp_blk_pct, opp_tov_pct) )
    conn.commit()

    ###### TOURNAMENT RESULTS ######
    print('...tournament results')
    # time.sleep(1)
    fhand = urllib.request.urlopen('https://www.sports-reference.com/cbb/postseason/' + str(year) + '-ncaa.html')
    
    count = 0
    if (year==2010):  # this year only had one play-in game, whereas subsequent years have four, a.k.a. the "First Four"
        cur.execute('SELECT id FROM School WHERE name = ? ', ('Winthrop', ))
        school_id1 = cur.fetchone()[0]
        cur.execute('INSERT OR IGNORE INTO Rank (season_id, school_id, tourn_rank) VALUES ( ?, ?, ? )', (season_id, school_id1, 16) )
        cur.execute('SELECT id FROM School WHERE name = ? ', ('Arkansas-Pine Bluff', ))
        school_id2 = cur.fetchone()[0]
        cur.execute('INSERT OR IGNORE INTO Rank (season_id, school_id, tourn_rank) VALUES ( ?, ?, ? )', (season_id, school_id2, 16) )
        cur.execute('INSERT OR REPLACE INTO Tournament (season_id, school_id1, school_id2, score1, score2) VALUES ( ?, ?, ?, ?, ? )',
                    (season_id, school_id1, school_id2, 44, 61) )
        count = count + 2
    
    # the toggle switch logic ensures three specific lines (rank, school, score) are found in precisely the correct order
    toggle0 = True;  toggle1 = False;  toggle2 = False;  toggle3 = False
    first4 = False;  pair = False
    for line in fhand:
        linetext = line.decode().strip()
        if toggle0:
            if '>Final Four</a>' in linetext:  # skip down to this line to start searching for relevant information
                toggle0 = False;  toggle1 = True
                continue
        else:
            if 'First Four</strong>' in linetext:
                first4 = True
            elif '<div id="bracket" class="team16">' in linetext:
                first4 = False
        if first4:
            rank_list = re.findall('<strong>([0-9]+)</strong>', linetext)
            if (len(rank_list)==2):
                rank = rank_list[0]
                school_list = re.findall('/cbb/schools/.+?>(.+?)</a>', linetext)
                if ('&amp;' in school_list[0]):  school_list[0] = school_list[0].replace('&amp;', '&')
                if ('&amp;' in school_list[1]):  school_list[1] = school_list[1].replace('&amp;', '&')
                if (school_list[0] in school_dict):  school_list[0] = school_dict[school_list[0]]  # rename schools to match stats tables
                if (school_list[1] in school_dict):  school_list[1] = school_dict[school_list[1]]  # rename schools to match stats tables
                
                cur.execute('SELECT id FROM School WHERE name = ? ', (school_list[0], ))
                try:
                    school_id1 = cur.fetchone()[0]
                except:
                    print('(!) WARNING:' + school_list[0] + ' must be added to school_dict')
                if (school_id1==None):  print('(!) WARNING:' + school_list[0] + ' must be added to school_dict')

                cur.execute('SELECT id FROM School WHERE name = ? ', (school_list[1], ))
                try:
                    school_id2 = cur.fetchone()[0]
                except:
                    print('(!) WARNING:' + school_list[1] + ' must be added to school_dict')
                if (school_id2==None):  print('(!) WARNING:' + school_list[1] + ' must be added to school_dict')

                cur.execute('INSERT OR IGNORE INTO Rank (season_id, school_id, tourn_rank) VALUES ( ?, ?, ? )', (season_id, school_id1, rank) )
                cur.execute('INSERT OR IGNORE INTO Rank (season_id, school_id, tourn_rank) VALUES ( ?, ?, ? )', (season_id, school_id2, rank) )
                score_list = re.findall('html.>([0-9]+)</a>', linetext)
                cur.execute('INSERT OR REPLACE INTO Tournament (season_id, school_id1, school_id2, score1, score2) VALUES ( ?, ?, ?, ?, ? )',
                            (season_id, school_id1, school_id2, score_list[0], score_list[1]) )
                count = count + 2
        elif toggle1:
            rank = re.findall('<span>([0-9]+)</span>', linetext)
            if len(rank)==1:
                toggle1 = False;  toggle2 = True
                rank = int(rank[0])
        elif toggle2:
            school_list = re.findall('/cbb/schools/.+?>(.+?)</a>', linetext)
            if len(school_list)==1:
                toggle2 = False;  toggle3 = True
                school = school_list[0]
            else:
                toggle2 = False;  toggle1 = True
        elif toggle3:
            score = re.findall('/cbb/boxscores/.+?>([0-9]+)</a>', linetext)
            if len(score)==1:
                toggle3 = False;  toggle1 = True
                score = int(score[0])
                if ('&amp;' in school):  school = school.replace('&amp;', '&')
                if (school in school_dict):  school = school_dict[school]  # rename schools to match stats tables

                cur.execute('SELECT id FROM School WHERE name = ? ', (school, ))
                try:
                    school_id = cur.fetchone()
                except:
                    print('(!) WARNING:' + school + ' must be added to school_dict')
                if (school_id==None):  print('(!) WARNING:' + school + ' must be added to school_dict')
                
                cur.execute('''INSERT OR IGNORE INTO Rank
                (season_id, school_id, tourn_rank) 
                VALUES ( ?, ?, ? )''',
                (season_id, school_id[0], rank) )
                if (pair):  # group team scores into pairs to save as game results
                    school_id2 = school_id[0]
                    score2 = score
                    cur.execute('''INSERT OR REPLACE INTO Tournament
                    (season_id, school_id1, school_id2, score1, score2) 
                    VALUES ( ?, ?, ?, ?, ? )''',
                    (season_id, school_id1, school_id2, score1, score2) )
                    pair = False
                else:
                    school_id1 = school_id[0]
                    score1 = score
                    pair = True
                count = count + 1
            else:
                toggle3 = False;  toggle1 = True
    conn.commit()
    print('   number of games: ' + str(count/2))
    if (pair): print('      (!) WARNING: not all teams were paired up')
    else:      print('    - all teams paired')

print('Data scraping successfully completed.')
