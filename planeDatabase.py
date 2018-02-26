"""
This has been written to target Python3
Wrapper or utility for the planeFinder application.
Need a database named planefinder with a user to access

TODO - fix the F in finder
Will create a table for every icao requested

"""

import planeUtil as pUtil
from pprint import pprint
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from psycopg2.extensions import AsIs
from time import mktime
from datetime import datetime
import sys

#This isn't private yet since it doesn't rely on anything from the self class. 
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
        conn = psycopg2.connect("dbname='planefinder' user='planefinder' host='unimog'")
    except:
        print('I am unable to connect to the database.')
        return -1
    return conn
"""End connect_to_plane_db"""

def get_dict_from_file(filename):
    """Create a dictionary from the original formated file. 
        A better ICD'd format will be created and followed.
        Takes in a filename and returns with a key of times and a value of a dictionary.     
        
        
    """
    
    try:
        file = open(filename,'r')
    except:
        print("Error opening:", filename)
        raise
    
    """
    Dictionary is used to store dictionaries. 
    Upper dict has a kay of the time and the value of the dict with the values
    Vales dict has keys of:
        lat, lon, alt, heading, vel, climbing, name
    """
    top_dict = {}
    for line in file:
        
        #get the time - first 17 characters and turn in into a time struct
        try:
            line_time = pUtil.custom_time_decode(line[0:17])
        except:
           print("Unexpected error:", sys.exc_info()[0])
           continue
        #find key associated with that time
        try:
            sub_dict = top_dict[line_time]
        except:
            #if key doesn't exist create it
            top_dict[line_time] = {}
            sub_dict = {}
            
        #split by spaces
        words = line.split()
        for word in words:
            #split by : -> if not 2 args then skip anyway
            subs = word.split(':')
            if len(subs) == 2:
                #if it starts whatever, put in that place
                if subs[0] == 'is':
                    sub_dict['name'] = subs[1]
                elif subs[0] == 'at':
                    ll = str(subs[1]).split(',')
                    sub_dict['lat'] = ll[0]
                    sub_dict['lon'] = ll[1]
                elif subs[0] == 'altitude': 
                    sub_dict['alt'] = subs[1]
                elif subs[0] == 'IC': 
                    sub_dict['IC'] = subs[1]
                elif subs[0] == 'heading':
                    sub_dict['dir'] = subs[1]
                elif subs[0] == 'vel':
                    sub_dict['vel'] = subs[1]
                elif subs[0] == 'climbing':
                    sub_dict['Vx'] = subs[1]
                else:    #TODO: Add the rest of the stuff
                    print(word)
                    pass # nothing to do with this line
        top_dict[line_time] = sub_dict
        
    file.close()
    return top_dict
"""End get_dict_from_file"""
   
#Beginning of a class
class dbConnection:
    """A class used to wrap the database functions"""
    
    def __init__(self):
        self.planes_seen = []
        self.connection = -1
        self.connect_to_plane_db()
        self.connection.autocommit = True
        if self.connection == -1:
            #deal with error
            exit()
        self.cur = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    """End __init___"""        
    
    def connect_to_plane_db(self):
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
            new_connection = psycopg2.connect("dbname='planefinder' user='planefinder' host='unimog'")
        except:
            print('I am unable to connect to the database.')
            #should probably raise an error
        self.connection = new_connection
    """End connect_to_plane_db"""    
    
    def check_icao(self, icao, create=True):
        """
        ICAO must be converted to lowercase before being submitted to the database. 
        Why? It is a postgresql thing

        Note:
            This checks for a table in the database that has a name equivalent to
            the icao string. If none exists, one is created unless specified otherwise.
            If the creation fails then False is returned.

        Args:
            icao (str): The ICAO(hex) in a string representation.
            create (bool): Will create icao table if true or not entered

        Returns:
            bool: True if made or exists. Returns true if not an error
        """
        
        if icao in self.planes_seen:
            return True
        
        # check if table exists. Return true if does
        self.cur.execute("select * from information_schema.tables where table_name=%s", (icao.lower(),))
        exists = bool(self.cur.rowcount)
        if exists:
            self.planes_seen.append(icao)
            return exists
        if not create:
            return exists
            
        # create table
        self.cur.execute(
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
        self.cur.execute("select * from information_schema.tables where table_name=%s", (icao.lower(),))
        exists = bool(self.cur.rowcount)
        if exists:
            self.planes_seen.append(icao)
            return exists

        #Failed to create new table. Just return false for now
        return False
    """End check_icao"""
    
    def insert_many(self, icao, time, name='', lat=361, lon=361, alt=-1, IC='', head=-1, vel=-1, Vx=''):
        """
        insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):

        Args:
            self (dbConnection): self reference
            icao (str): The ICAO(hex) in a string representation.
            time (time): Time that the event took place
            name (str, optional): Name value to insert
            lat (float, optional): Latitude value to insert
            lon (float, optional): Longitude value to insert
            alt (int, optional): Altitude (in feet) value to insert
            IC (str, optional): If given, will check to see if matches icao
            head (float, optional): Direction (in degrees) value to insert
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

        self.check_icao(icao)
        dt = datetime.fromtimestamp(mktime(time))
        
        #insert name
        #pd.insert(cur,ic,t,name=str(id))

        #insert lat lon alt
        #pd.insert(cur,ic,t,lat=lat,lon=lon,alt=alt)

        #insert heading velocity updown
        #pd.insert(cur,ic,t,head=vel[1],vel=vel[0],Vx=vel[2])
        
        if IC != '':
            if IC.lower() != icao.lower():
                print("ICAO missmatch!", icao.lower(), "!=", IC.lower())
        if name != '':
            self.cur.execute(
                sql.SQL("""
                    INSERT INTO {} (time,name)
                    VALUES (%s,%s)
                    ON CONFLICT (time) DO UPDATE
                    SET name=EXCLUDED.name
                """).format(sql.Identifier(icao.lower())), (dt, name))
        elif lat != 361:
            self.cur.execute(
                sql.SQL("""
                    INSERT INTO {} (time,lat,lon,alt)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (time) DO UPDATE
                    SET lat=EXCLUDED.lat,
                    lon=EXCLUDED.lon,
                    alt=EXCLUDED.alt
                """).format(sql.Identifier(icao.lower())), (dt, lat, lon, alt))
        elif head != -1:
            self.cur.execute(
                sql.SQL("""
                    INSERT INTO {} (time,dir, vel, vx)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (time) DO UPDATE
                    SET dir=EXCLUDED.dir, 
                    vel=EXCLUDED.vel,
                    vx=EXCLUDED.vx
                """).format(sql.Identifier(icao.lower())), (dt, head, vel, Vx))           
    """End insert"""

    def insert(self, icao, time, name='', lat=361, lon=361, alt=-1, IC='', head=-1, vel=-1, Vx=''):
        """
        insert(cur,icao,time,name='',lat=361,lon=361,alt=-1,IC='',dir=-1,vel=-1,Vx=''):

        Args:
            self (dbConnection): self reference
            icao (str): The ICAO(hex) in a string representation.
            time (time): Time that the event took place
            name (str, optional): Name value to insert
            lat (float, optional): Latitude value to insert
            lon (float, optional): Longitude value to insert
            alt (int, optional): Altitude (in feet) value to insert
            IC (str, optional): If given, will check to see if matches icao
            head (float, optional): Direction (in degrees) value to insert
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

        self.check_icao(icao)
        dt = datetime.fromtimestamp(mktime(time))
        keys = ('time',)
        values = (dt,)
        set = ()
        if IC != '':
            if IC.lower() != icao.lower():
                print("ICAO missmatch!", icao.lower(), "!=", IC.lower())
        if name != '':
            keys = keys + ('name',)
            values = values + (name,)
            set = set + ('name=EXCLUDED.name \n',)
        if lat != 361:
            keys = keys + ('lat',)
            values = values + (lat,)
            set = set + ('lat=EXCLUDED.lat \n',)
        if lon != 361:
            keys = keys + ('lon',)
            values = values + (lon,)
            set = set + ('lon=EXCLUDED.lon \n',)
        if alt != -1:
            keys = keys + ('alt',)
            values = values + (alt,)
            set = set + ('alt=EXCLUDED.alt \n',)
        if head != -1:
            keys = keys + ('dir',)
            values = values + (head,)
            set = set + ('dir=EXCLUDED.dir \n',)
        if vel != -1:
            keys = keys + ('vel',)
            values = values + (vel,)
            set = set + ('vel=EXCLUDED.vel \n',)
        if Vx != '':
            keys = keys + ('vx',)
            values = values + (Vx,)
            set = set + ('vx=EXCLUDED.vx \n',)
            
        k="(" + ", ".join( str(x) for x in keys) + ")"
        v="(\'" + "\', \'".join( str(x) for x in values) + "\')"
        s="SET " + ", ".join( str(x) for x in set) + ""
        self.cur.execute(
            sql.SQL("""
                    INSERT INTO {} %s
                    VALUES %s
                    ON CONFLICT (time) DO UPDATE
                    %s
                    """).format(sql.Identifier(icao.lower())), (AsIs(k), AsIs(v), AsIs(s)))
    """End insert"""
    
    def get_dict_from_timeframe(self, icao, start=-1, end=-1):
        """
        Returns a dictionary with values that are contained within whatever range
        Args:
            self (dbConnection): self reference
            icao (str): The ICAO(hex) in a string representation.
            start (time_struct, optional): Time to start the retrieval from
            end (time_struct, optional): Time to end the retrieval

        Returns:
            dict: dictionary containing time as the key and dictionary of the values
                for the selected time frame

        Examples:
            To retrieve a dictionary with all of the information for as
            certain icao:

            >>> get_dict_from_timeframe(cur,icao)
        Note:
            Timestamps need to be in time_struct format
            SQL:
            select * from ADDC1A where time > make_timestamp(2017,12,22,15,0,0)
            and time < make_timestamp(2017,12,22,16,0,0)
        """

        if start == -1 and end == -1:
            #return all
            self.cur.execute(
                sql.SQL("""
                    SELECT * FROM {} ORDER BY time
                """).format(sql.Identifier(icao.lower())))
        elif start == -1:
            #return all up to end
            dt_end = datetime.fromtimestamp(mktime(end))
            self.cur.execute(
                sql.SQL("""
                    SELECT * FROM {}
                    WHERE time < %s
                    ORDER BY time
                """).format(sql.Identifier(icao.lower())), (dt_end,))
        elif end == -1:
            #return all from start on
            dt_start = datetime.fromtimestamp(mktime(start))
            self.cur.execute(
                sql.SQL("""
                    SELECT * FROM {}
                    WHERE time > %s
                    ORDER BY time
                """).format(sql.Identifier(icao.lower())), (dt_start,))
        else:
            #return some both
            dt_start = datetime.fromtimestamp(mktime(start))
            dt_end = datetime.fromtimestamp(mktime(end))
            self.cur.execute(
                sql.SQL("""
                    SELECT * FROM {}
                    WHERE time > %s and time < %s
                    ORDER BY time
                """).format(sql.Identifier(icao.lower())), (dt_start, dt_end))

        res = self.cur.fetchall()
        ret = {}
        #TODO: Key on TIME
        for row in res:
            #ret.append(dict(row))
            td = dict(row)
            time = td['time']
            del td['time']
            ret[time]=td
            
        return ret
    """End get_dict_from_timeframe"""

    def get_dict(self, icao):
        """
        Wrapper to make it simpler to get all values for an ICAO
        into a dictionary
        Args:
            self (dbConnection): self reference
            icao (str): The ICAO(hex) in a string representation.

        Returns:
            dict: dictionary containing time as the key and dictionary of the values
                for the selected time frame
        """
        return self.get_dict_from_timeframe(icao)
    """End get_dict"""

    def get_all_tables(self):
        """
        Function returns a list of all tables contained in the database referred to by
        the input cursor.

        Args:
            self (dbConnection): self reference

        Returns:
            list: A list of public table names in the current database.
                When used on planefinder, this is the list of the icao's that
                are stored in the database.

        Todo:
            Make sure it is actually a list.
        """
        self.cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE'
                    ORDER BY table_name
                    """)
        res = self.cur.fetchall()
        ret = []
        for row in res:
            ret.append(row[0])

        return ret
    """End get_all_tables"""    
    
    def get_all_tables(self):
        """
        Function returns a list of all tables contained in the database referred to by
        the input cursor.

        Args:
            self (dbConnection): self reference

        Returns:
            list: A list of public table names in the current database.
                When used on planefinder, this is the list of the icao's that
                are stored in the database.

        Todo:
            Make sure it is actually a list.
        """
        self.cur.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    AND table_type='BASE TABLE'
                    ORDER BY table_name
                    """)
        res = self.cur.fetchall()
        ret = []
        for row in res:
            ret.append(row[0])

        return ret
    """End get_all_tables"""    
    
    def get_connection_isopen(self):
        #return if it is connected4
        #Read-only integer attribute: 0 if the connection is open, nonzero if it is closed or broken.
        return (not self.connection.closed)
    """End get_connection_isopen"""    
    
    def close_db_connection(self):
        #return if it is connected4
        self.connection.close()
        return self.connection.closed
    """End close_db_connection"""
    
"""End dbConnection class"""