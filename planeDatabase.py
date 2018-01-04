"""
This has been written to target Python3
Wrapper or utility for the planeFinder application. 

Need a database named planefinder with a user to access

TODO - fix the F in finder
Will create a table for every icao

"""
import psycopg2
import psycopg2.extras
import planeUtil as pUtil
from psycopg2 import sql
from time import mktime
from datetime import datetime
from pprint import pprint


def connect_to_plane_db():
    #This connects to the db and return the connection
    #TODO: Don't have the password in code
    try:
        conn=psycopg2.connect("dbname='planefinder' user='planeFinder' host='localhost'")    
    except:
        print('I am unable to connect to the database.')
        return -1
    return conn

"""End connect_to_plane_db"""


"""check_icao(cur, icao)

ICAO must be converted to lowercase before being submitted to the database. Why?

input: icao
output: bool if made or exists. Returns true if not an error
"""

def check_icao(cur, icao):
    # check if table exists. Return true if does
    cur.execute("select * from information_schema.tables where table_name=%s", (icao.lower(),))
    exists = bool(cur.rowcount)
    if exists:
        return exists

    # create table
    cur.execute(
        sql.SQL("""
            CREATE TABLE {}(
            TIME TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY NOT NULL,
            NAME CHAR(10),
            LAT DOUBLE PRECISION,
            LON DOUBLE PRECISION,
            ALT INTEGER,
            DIR DOUBLE PRECISION,
            VEL INTEGER,
            VX INTEGER)
            """).format(sql.Identifier(icao.lower())))

    #check to see if table exists now
    cur.execute("select * from information_schema.tables where table_name=%s", (icao.lower(),))
    exists = bool(cur.rowcount)
    if exists:
        return exists

    #now we have some problems. Just return false for now
    return False

"""End check_icao"""



"""
insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):

input: insert(icao,time,name='',lat=360,lon=360,alt=-1,IC='',dir=-1,vel=-1,Vx='')
output: true if good, false if bad

If IC is input, IC is checked against icao
If values aren't default then they are updated.


INSERT INTO $icao (time,othernames,......)
VALUES ($time, $.......)
ON CONFLICT (time) DO UPDATE
SET parag=EXCLUDED.arg

"""

def insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):
    
    check_icao(cur, icao)
    dt = datetime.fromtimestamp(mktime(time))

    if IC != '':
        if IC.lower() != icao.lower():
            print("ICAO missmatch!",icao.lower(),"!=",IC.lower())
    if name != '':
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,name)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET name=EXCLUDED.name
            """).format(sql.Identifier(icao.lower())),(dt,name))
    if lat != 361:
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,lat)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET lat=EXCLUDED.lat
            """).format(sql.Identifier(icao.lower())),(dt,lat))
    if lon != 361:
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,lon)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET lon=EXCLUDED.lon
            """).format(sql.Identifier(icao.lower())),(dt,lon))
    if alt != -1:
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,alt)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET alt=EXCLUDED.alt
            """).format(sql.Identifier(icao.lower())),(dt,alt))
    if dir != -1:
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,dir)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET dir=EXCLUDED.dir
            """).format(sql.Identifier(icao.lower())),(dt,dir))
    if vel != -1:
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,vel)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET vel=EXCLUDED.vel
            """).format(sql.Identifier(icao.lower())),(dt,vel))
    if Vx != '':
        cur.execute(
            sql.SQL("""
                INSERT INTO {} (time,vx)
                VALUES (%s,%s)
                ON CONFLICT (time) DO UPDATE
                SET vx=EXCLUDED.vx
            """).format(sql.Identifier(icao.lower())),(dt,Vx))
    
"""End insert"""



"""
Returns a dictionary with values that are contained within whatever range
input: cursor, icao, start timestamp, end timestamp

Timestamps need to be in time_struct format

SQL:
#select * from ADDC1A where time > make_timestamp(2017,12,22,15,0,0) and time < make_timestamp(2017,12,22,16,0,0)
"""
def dict_from_timeframe(cur,icao,start=-1,end=-1):


    if start == -1 and end == -1:
        #return all
        cur.execute(
            sql.SQL("""
                SELECT * FROM {} ORDER BY time
            """).format(sql.Identifier(icao.lower())))
    elif start == -1:
        #return all up to end
        dt_end = datetime.fromtimestamp(mktime(end))
        cur.execute(
            sql.SQL("""
                SELECT * FROM {}
                WHERE time < %s
                ORDER BY time
            """).format(sql.Identifier(icao.lower())),(dt_end,))
    elif end == -1:
        #return all from start on
        pass
        dt_start = datetime.fromtimestamp(mktime(start))
        cur.execute(
            sql.SQL("""
                SELECT * FROM {}
                WHERE time > %s
                ORDER BY time
            """).format(sql.Identifier(icao.lower())),(dt_start,))
    else:
        #return some both
        dt_start = datetime.fromtimestamp(mktime(start))
        dt_end = datetime.fromtimestamp(mktime(end))
        cur.execute(
            sql.SQL("""
                SELECT * FROM {}
                WHERE time > %s and time < %s
                ORDER BY time
            """).format(sql.Identifier(icao.lower())),(dt_start,dt_end))

    res = cur.fetchall()
    ret = []
    #TODO: Key on TIME
    for row in res:
        ret.append(dict(row))

    return ret

"""End dict_from_timeframe"""

"""
Wrapper to make it simpler to get all values for an ICAO
into a dictionary
"""
def get_dict(cur,icao):
    return dict_from_timeframe(cur,icao)
"""End get_dict"""


def get_all_tables(cur):
    cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema='public'
                AND table_type='BASE TABLE'
                ORDER BY table_name
                """)
    res = cur.fetchall()
    ret = []
    for row in res:
        ret.append(row[0])

    return ret

"""End get_all_tables"""


#SELECT count(table_name) FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name
                
