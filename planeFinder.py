#!/usr/bin/env python3
#This has been rewritten to target Python3

import sys
import pyModeS as pms
import socket
import planeDatabase as pd
import planeUtil as pUtil

#TODO: Figure out how defs work
#TODO: Figure a better way to do this
#Read one line from the socket
def getLine(socket):
    ret = ""
    while True:
        char = socket.recv(1).decode()
        #print("Got a byte:",str(char))
        if str(char) == str('\n'):
            return ret
        elif str(char) != '\n' and str(char) != '*' and char != ';':
            ret+=str(char)
    return ret
    
    
if __name__ == '__main__':    
    ref_lon = -76.600144
    ref_lat = 39.070902
    num_args = len(sys.argv)
    #Lines to ignore in config file. 
    ignore_lines = ('#','\n')
    #This is defined in ADSB standard
    identifier_string = "#ABCDEFGHIJKLMNOPQRSTUVWXYZ#####_###############0123456789######"
    config_file = ""
    ip_addr = ""
    file_directory="data"
    #default port is 30002
    port = 30002
    skip = 0
    desiredIC = ()
    to_write = 0
    silent = 0
    use_database = 0
    dbc = -1
    write_to_file = 0

    #read in arguments 
    for x in range(0,num_args):
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
        print("Error opening config file:",config_file)
        sys.exit()
        
        
    for line in f:
        if line.startswith(ignore_lines):
            #print "Ignoring line:", line
            pass
        elif len(line) <= 2:
            # This is a blank line with only newline chars probably so skip
            #print "Ignoring blank line:", line
            pass
        elif line.startswith("icao:"):
            arr=line.split(":")
            #add the second element, first is icao
            desiredIC+=(''.join([i for i in arr[1] if pUtil.ishex(i)]),)
            #TODO: Find a better way to strip off unwanted return characters
        elif line.startswith("reg:"):
            #print "Reg not implemented yet, not adding:",line
            pass
        else:
            print("Unknown config line:",line," ",len(line))

    f.close()
    
    if not silent:
        print("List of icao:",desiredIC)

    # Connect to network connection for raw data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.connect((ip_addr,port))
    except socket.gaierror:
        # this means could not connect
        print("There was an error connecting to ", ip_addr,":",port)
        sys.exit()

    if not silent:
        print("Successfully connected to ", ip_addr,":",port)
    
    #connect to database        
    if use_database == 1:
        dbc = pd.dbConnection() 
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
        line = getLine(s)
        
        #get the icao
        ic = pms.adsb.icao(line)
        
        if ic in desiredIC:
            if not silent:
                print("Found desired IC:",ic)
            to_write=1
        
        

        #Open the IC file
        # TODO: Better way to write in designated place
        #ic_file="C:\\Users\\chuck\\Documents\\python\\planeFinder\\data\\"+ic+".txt"
        if write_to_file == 1:
            ic_file=file_directory+"/"+ic+".txt"
            ic_f = open(ic_file, "a")
            
        # Initialize the lineout
        lineout = line
        t = pUtil.time.gmtime()
        timeout = pUtil.time.strftime("%Y%m%dT%H:%M:%S",t)
        dt = pUtil.datetime.fromtimestamp(pUtil.time.mktime(t))
        
        df = pms.adsb.df(line)
        
        #Begin the decoder
        if df == 0:
            #this is only an icao number and identifier, nothing to log
            pass
        elif df == 11:
            #this is only an icao number and identifier, nothing to log
            pass
        elif (df == 17 or df == 18):
            tc = pms.adsb.typecode(line)
            #this is a ADS-B message 
            if tc >= 1 and tc <= 4:
                #identifier
                id = pms.adsb.callsign(line)
                lineout = "IC:"+str(ic)+" is:"+str(id)
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic,t,name=str(id))
            elif tc >= 5 and tc <= 8:
                #surface positions - compact position reporting
                #do with only one message since we have the reference location
                #(lat,lon) = pms.adsb.airborne_position_with_ref(line, ref_lat, ref_lon)
                #alt = pms.adsb.altitude(line)
                #lineout = "IC:"+str(ic)+" is at:"+str(lat)+","+str(lon)+" and altitude:"+str(alt)+" ft. SP"
                #to_write = 1
                #if dbc != -1: #use_database == 1:
                #    dbc.insert(ic,t,lat=lat,lon=lon,alt=alt)
                pass
            elif tc >= 9 and tc <= 18:
                #airborne positions - "position message"
                #do with only one message since we have the reference location
                (lat,lon) = pms.adsb.airborne_position_with_ref(line, ref_lat, ref_lon)
                alt = pms.adsb.altitude(line)
                lineout = "IC:"+str(ic)+" is at:"+str(lat)+","+str(lon)+" and altitude:"+str(alt)+" ft."
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic,t,lat=lat,lon=lon,alt=alt)
            elif tc == 19:
                #airborne velocity
                #s_head = pms.adsb.speed_heading(line)
                #s_v = pms.adsb.surface_velocity(line)
                vel = pms.adsb.velocity(line)
                lineout="IC:"+str(ic)+" heading:"+str(vel[1])+" vel:"+str(vel[0])+" kt climbing:"+str(vel[2])+" ft/min. "+str(vel[3])
                to_write = 1
                if dbc != -1: #use_database == 1:
                    dbc.insert(ic,t,head=vel[1],vel=vel[0],Vx=vel[2])
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
        else:
            #This is a format that isn't implemented yet
            pass

        lineout=timeout+" "+lineout
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

        if write_to_file:
            ic_f.close()

        to_write = 0
        #log data if it matches the icao in list

        
    #TODO: After exiting main loop do something useful

    s.close()





