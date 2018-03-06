#!/usr/bin/env python3
"""planeFinder main process.

This module is used to read in ADSB strings and perform various functions
with the date. Currently writing to file and writing to a postgres database
are the accepted outputs. This module targets python3

Example:
    Examples can be given using either the ``Example`` or ``Examples``
    sections. Sections support any reStructuredText formatting, including
    literal blocks::

        $ python3 example_google.py

Section breaks are created by resuming unindented text. Section breaks
are also implicitly created anytime a new section starts.

Attributes:
    none

Todo:
    * do argparse

.. _Google Python Style Guide:
   http://google.github.io/styleguide/pyguide.html

"""

import sys
import socket
import pyModeS as pms
import planeDatabase as pd
import planeUtil as pUtil


#TODO: Figure out how to do constants correctly
IGNORE_LINES = ('#', '\n')
#This is defined in ADSB standard
IDENTIFIER_STRING = "#ABCDEFGHIJKLMNOPQRSTUVWXYZ#####_###############0123456789######"


#TODO: Figure out how defs work
#TODO: Figure a better way to do this
def get_line(socket):
    """
    This function reads in hex strings from a socket.

     Note:
        For information about the database setup, please see wiki.

    Args:
        socket (socket): Network socket to read in from

    Returns:
        str: String of hex characters representing a asdb string.
    """


    ret = ""
    while True:
        char = socket.recv(1).decode()
        #print("Got a byte:",str(char))
        if str(char) == str('\n'):
            return ret
        elif str(char) != '\n' and str(char) != '*' and char != ';':
            ret += str(char)
    return ret


def main():
    """
    This function is the main function of the plane finer program

     Note:
        This will read in and process adsb data.

    Args:
        none

    Returns:
        none
    """
    ref_lon = -76.600144
    ref_lat = 39.070902
    num_args = len(sys.argv)
    #Lines to ignore in config file.
    config_file = ""
    ip_addr = ""
    file_directory = "data"
    #default port is 30002
    port = 30002
    skip = 0
    desired_icao = ()
    to_write = 0
    silent = 0
    use_database = 0
    dbc = -1
    write_to_file = 0

    #read in arguments
    #TODO: use argparse instead
    for x in range(0, num_args):
        if skip == 1:
            skip = 0
        elif x == 0: # skip this since it is the function callable
            #print("Function called is:",sys.argv[x])
            pass
        elif sys.argv[x] == "-c":
            config_file = sys.argv[x+1]
            skip = 1
        elif sys.argv[x] == "-i":
            ip_addr = sys.argv[x+1]
            skip = 1
        elif sys.argv[x] == "-p":
            port = int(sys.argv[x+1])
            skip = 1
        elif sys.argv[x] == "-s":
            silent = 1
            skip = 0
        elif sys.argv[x] == "-d":
            use_database = 1
            skip = 0
        elif sys.argv[x] == "-f":
            write_to_file = 1
            skip = 0
        elif sys.argv[x] == "-w":
            file_directory = sys.argv[x+1]
            skip = 1
        else:
            print("Unknown argument:", sys.argv[x])

    #Read in config
    try:
        f = open(config_file, "r") #opens file with name of "test.txt"
    except:
        print("Error opening config file:", config_file)
        sys.exit()


    for line in f:
        if line.startswith(IGNORE_LINES):
            #print "Ignoring line:", line
            pass
        elif len(line) <= 2:
            # This is a blank line with only newline chars probably so skip
            #print "Ignoring blank line:", line
            pass
        elif line.startswith("icao:"):
            arr = line.split(":")
            #add the second element, first is icao
            desired_icao += (''.join([i for i in arr[1] if pUtil.ishex(i)]),)
            #TODO: Find a better way to strip off unwanted return characters
        elif line.startswith("reg:"):
            #print "Reg not implemented yet, not adding:",line
            pass
        else:
            print("Unknown config line:", line, " ", len(line))

    f.close()

    if not silent:
        print("List of icao:", desired_icao)

    # Connect to network connection for raw data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip_addr, port))
    except socket.gaierror:
        # this means could not connect
        print("There was an error connecting to ", ip_addr, ":", port)
        sys.exit()

    if not silent:
        print("Successfully connected to ", ip_addr, ":", port)

    #connect to database
    if use_database == 1:
        dbc = pd.DBConnection()
        if dbc.get_connection_isopen():
            if not silent:
                print("Database connection open")
        else:
            dbc = -1
            if not silent:
                print("Database connection not open")


    # Main while loop
    #TODO: Exit gracefully
    while True:
        line = get_line(s)

        # Initialize the lineout
        lineout = line
        t = pUtil.time.gmtime()
        timeout = pUtil.time.strftime("%Y%m%dT%H:%M:%S", t)
        #dt = pUtil.datetime.fromtimestamp(pUtil.time.mktime(t))

        df = pms.adsb.df(line)

        #Begin the decoder
        if df == 0:
            #this is only an icao number and identifier, nothing to log
            pass
        elif (df == 4 or df == 5):
            continue
            #this is for ehs
            #calculate the ICAO address from an Mode-S message with DF4, DF5, DF20, DF21
            #ic_e=pms.ehs.icao(line)
            #lineout = "IC:"+str(ic_e)+" for DF"+str(df)
            #to_write = 1
            pass
        elif df == 11:
            #this is only an icao number and identifier, nothing to log
            pass
        elif (df == 17 or df == 18):
            #do stuff for the IC
            ic = pms.adsb.icao(line)
            #TODO: put the following in a function since it is repeated for 20/21
            if ic in desired_icao:
                if not silent:
                    print("Found desired IC:", ic)
                to_write = 1
            #Open the IC file
            if write_to_file == 1:
                ic_file = file_directory+"/"+ic+".txt"
                ic_f = open(ic_file, "a")

            tc = pms.adsb.typecode(line)
            #this is a ADS-B message
            if tc >= 1 and tc <= 4:
                #identifier
                callsign = pms.adsb.callsign(line)
                lineout = "IC:"+str(ic)+" is:"+str(callsign)
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic, t, name=str(callsign))
            elif tc >= 5 and tc <= 8:
                #surface positions - compact position reporting
                #do with only one message since we have the reference location
                #(lat,lon) = pms.adsb.airborne_position_with_ref(line, ref_lat, ref_lon)
                #alt = pms.adsb.altitude(line)
                #lineout = "IC:"+str(ic)+" is at:"+str(lat)+","+str(lon)+"
                #and altitude:"+str(alt)+" ft. SP"
                #to_write = 1
                #if dbc != -1: #use_database == 1:
                #    dbc.insert(ic,t,lat=lat,lon=lon,alt=alt)
                pass
            elif tc >= 9 and tc <= 18:
                #airborne positions - "position message"
                #do with only one message since we have the reference location
                (lat, lon) = pms.adsb.airborne_position_with_ref(line, ref_lat, ref_lon)
                alt = pms.adsb.altitude(line)
                lineout = "IC:"+str(ic)+" is at:"+str(lat)+","+str(lon)+" and altitude:"+str(alt)+" ft."
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic, t, lat=lat, lon=lon, alt=alt)
            elif tc == 19:
                #airborne velocity
                #s_head = pms.adsb.speed_heading(line)
                #s_v = pms.adsb.surface_velocity(line)
                vel = pms.adsb.velocity(line)
                lineout = "IC:"+str(ic)+" heading:"+str(vel[1])+" vel:"+str(vel[0])+" kt climbing:"+str(vel[2])+" ft/min. "+str(vel[3])
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic, t, head=vel[1], vel=vel[0], Vx=vel[2])
            elif tc >= 20 and tc <= 22:
                #Airborne position (w/ GNSS Height)
                pass
            elif tc >= 23 and tc <= 30:
                #reserved
                pass
            elif tc == 31:
                #Aircraft Operation Status
                pass
            else:
                #Should never get to this.
                pass
        elif (df == 20 or df == 21):
            #this is for ehs
            ic_e = pms.ehs.icao(line) #calculate the ICAO address from an Mode-S message with DF4, DF5, DF20, DF21
            #TODO: put the following in a function since it is repeated for 20/21
            if ic_e in desired_icao:
                if not silent:
                    print("Found desired IC:", ic_e)
                to_write = 1
            #Open the IC file
            if write_to_file == 1:
                ic_file = file_directory+"/"+ic_e+".txt"
                ic_f = open(ic_file, "a")

            bds = pms.ehs.BDS(line)
            if bds == 'BDS20':
                #decode the callsign in the same way as the non ehs method
                #id = pms.adsb.callsign(line)
                callsign = pms.ehs.callsign(line)
                lineout = "IC:"+str(ic_e)+" is:"+str()
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic_e, t, name=str(callsign))
            elif bds == 'BDS40':
                alt_fms = pms.ehs.alt40fms(line) #Selected altitude, FMS
                alt_mcp = pms.ehs.alt40mcp(line) #Selected altitude, MCP/FCU
                baro = pms.ehs.p40baro(line) #Barometric pressure setting


                lineout = "IC:"+str(ic_e)+" fms:"+str(alt_fms)+" ft mcp:"+str(alt_mcp)+" ft baro:"+str(baro)+" millibar"
                to_write = 1
                if dbc != -1: #use_database == 1:
                    #dbc.insert(ic_e,t,name=str(id))
                    pass
            elif bds == 'BDS50':
                #True track angle, BDS 5,0 message angle in degrees to true north (from 0 to 360)
                trk = pms.ehs.trk50(line)
                #Track angle rate, BDS 5,0 message angle rate in degrees/second
                rtrk = pms.ehs.rtrk50(line)
                #Roll angle, BDS 5,0 message in degrees negative->left wing down, positive->right wing down
                rang = pms.ehs.roll50(line)
                #Aircraft true airspeed, BDS 5,0 message
                airsp = pms.ehs.tas50(line)
                #Ground speed, BDS 5,0 message
                gndsp = pms.ehs.gs50(line)

                lineout = "IC:"+str(ic_e)+" trk:"+str(trk)+" deg rtrk:"+str(rtrk)+" deg/sec rang:"+str(rang)+" deg airsp:"+str(airsp)+" kt gndsp:"+str(gndsp)+" kt"
                to_write = 1
                if dbc != -1: #use_database == 1:
                    #dbc.insert(ic_e,t,name=str(id))
                    pass
            elif bds == 'BDS60':
                #Megnetic heading of aircraft deg
                hdg = pms.ehs.hdg60(line)
                #Indicated airspeed
                airsp = pms.ehs.ias60(line)
                #Aircraft MACH number
                mach = pms.ehs.mach60(line)
                #Vertical rate ft/min from barometric measurement
                vbaro = pms.ehs.vr60baro(line)
                #Vertical rate ft/min messured by onbard equiments (IRS, AHRS)
                vins = pms.ehs.vr60ins(line)
                lineout = "IC:"+str(ic_e)+" hdg:"+str(hdg)+" deg airsp:"+str(airsp)+" kt mach:"+str(mach)+" vbaro:"+str(vbaro)+" ft/min vins:"+str(vins)+" ft/min"
                to_write = 1
                if dbc != -1: #use_database == 1:
                    #dbc.insert(ic_e,t,name=str(id))
                    pass
        else:
            #This is a format that isn't implemented yet
            pass

        lineout = timeout+" "+lineout
        sout = str(lineout)

        # Always print output
        # Need to fix this up to make it more operational
        if to_write:
            if not silent:
                print(sout)

        # If print to file
        if to_write and write_to_file:
            ic_f.write(sout)
            ic_f.write("\n")#to make a newline. Done automatically in print

        #need to make this smarter. It is queing when it should not
        if ic_f:
            ic_f.close()

        to_write = 0
        #log data if it matches the icao in list


    #TODO: After exiting main loop do something useful

    s.close()

if __name__ == '__main__':
    main()
