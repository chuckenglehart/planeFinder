"""
This has been written to target Python3
Wrapper or utility for the planeFinder application. 

Need a database named planefinder with a user to access

TODO - fix the F in finder
Will create a table for every icao requested

"""
import psycopg2
import psycopg2.extras
import planeUtil as pUtil
from psycopg2 import sql
from time import mktime
from datetime import datetime
from pprint import pprint


def connect_to_plane_db():
    """
    This function provides the connection to the planeFinder database. 

     Note:
        For information about the database setup, please see wiki.
    Args:
        none: This may change in the future
        
    Returns:
        conn: SQL connection from the psycopg2 lib

    """
    #This connects to the db and return the connection
    #TODO: Don't have the password in code
    try:
        conn=psycopg2.connect("dbname='planefinder' user='planeFinder' host='localhost'")    
    except:
        print('I am unable to connect to the database.')
        return -1
    return conn
"""End connect_to_plane_db"""


def check_icao(cur, icao):    
    """

    ICAO must be converted to lowercase before being submitted to the database. Why?

    Note:
        This checks for a table in the database that has a name equivalent to 
        the icao string. If none exists, one is created. If the creation fails
        then False is returned.
    
    Args:
        cur: (cursor): Connection cursor from psycopg2
        icao (str): The ICAO(hex) in a string representation. 
        
    Returns:
        bool: True if made or exists. Returns true if not an error

    """
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


def insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):    
    """
    insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):

    Args:
        cur: (cursor): Connection cursor from psycopg2
        icao (str): The ICAO(hex) in a string representation. 
        time (time): Time that the event took place
        name (str, optional): Name value to insert
        lat (float, optional): Latitude value to insert
        lon (float, optional): Longitude value to insert
        alt (int, optional): Altitude (in feet) value to insert
        IC (str, optional): If given, will check to see if matches icao
        dir (float, optional): Direction (in degrees) value to insert
        vel (int, optional): Velocity (in MPH) value to insert
        Vx (int, optional): Rate fo altitude change (in feet/sec) to insert
    
    Returns:
        bool: true if good, false if bad

    Note:
        SQL query modeled after:
        INSERT INTO $icao (time,othernames,......)
        VALUES ($time, $.......)
        ON CONFLICT (time) DO UPDATE
        SET parag=EXCLUDED.arg
   
    Todo:
        Merge the many sql inserts into one
    """

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


def dict_from_timeframe(cur,icao,start=-1,end=-1):
    """
    Returns a dictionary with values that are contained within whatever range
    Args:
        cur: (cursor): Connection cursor from psycopg2
        icao (str): The ICAO(hex) in a string representation.  
        start (time_struct, optional): Time to start the retrieval from
        end (time_struct, optional): Time to end the retrieval

    Returns:
        dict: dictionary containing time as the key and dictionary of the values
            for the selected time frame
            
    Examples:
        To retrieve a dictionary with all of the information for as
        certain icao:
            
        >>> dict_from_timeframe(cur,icao)
    Note:        
        Timestamps need to be in time_struct format
        SQL:
        select * from ADDC1A where time > make_timestamp(2017,12,22,15,0,0) and time < make_timestamp(2017,12,22,16,0,0)
    """

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


def get_dict(cur,icao):
    """
    Wrapper to make it simpler to get all values for an ICAO
    into a dictionary
    Args:
        cur: (cursor): Connection cursor from psycopg2
        icao (str): The ICAO(hex) in a string representation.

    Returns:
        dict: dictionary containing time as the key and dictionary of the values
            for the selected time frame
    """
    return dict_from_timeframe(cur,icao)
"""End get_dict"""


def get_all_tables(cur):
    """
    Function returns a list of all tables contained in the database referred to by
    the input cursor.
    
    Args:
        cur: (cursor): Connection cursor from psycopg2
    
    Returns:
        list: A list of public table names in the current database. 
            When used on planefinder, this is the list of the icao's that
            are stored in the database. 
        
    Todo:
        Make sure it is actually a list.
    """
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
