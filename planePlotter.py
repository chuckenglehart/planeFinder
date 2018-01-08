#!/usr/bin/python3 
""" Written to target Python3
Requires gmplot to be installed. 
"""

import planeUtil as pUtil
import argparse
import gmplot
from pprint import pprint # for pretty dictionary printing. 


def create_dict_original(filename):
    """Create a dictionary from the original formated file. 
        A better ICD'd format will be created and followed.
        Takes in a filename and returns with a key of times and a value of a dictionary.     
    """
    
    #open file 
    file = open(filename,'r')
    
    
    """
    Dictionary is used to store dictionaries. 
    Upper dict has a kay of the time and the value of the dict with the values
    Vales dict has keys of:
        lat, lon, alt, heading, vel, climbing, name
    """
    top_dict = {}
    for line in file:
        
        #get the time - first 17 characters and turn in into a time struct
        line_time = pUtil.custom_time_decode(line[0:17])
        
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


def create_dict_from_db(icao):
    #connect to db

    #get cursor

    #use get_dict function
    pass

if __name__ == '__main__':    

    """Create the command line argument parser. This is also used to 
    give the help functionality without having to build it manually. 
    TODO: Figure out why the required arguments show up under optional. 
    """
    parser = argparse.ArgumentParser(description='Create a map from data file.')
    parser.add_argument('-i', '--icao', nargs=1, required=True, help='ICAO number to create a map from')
    parser.add_argument('-o', '--output', nargs=1, type=argparse.FileType('w'), help='output filename')
    parser.add_argument('-s', '--start', nargs=1, type=pUtil.custom_time_decode, help='start time YYYYMMDDTHH:MM:SS')
    parser.add_argument('-e', '--end', nargs=1, type=pUtil.custom_time_decode, help='end time YYYYMMDDTHH:MM:SS')
    parser.add_argument('-d', '--debug', action='store_true', default=False)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.1')
    args = parser.parse_args()
    print(args)
        
    debug = args.debug
    
    icao = args.icao[0]
    filename="data/"+icao+".txt"
    
    try:
        #get the name from an io txt wrapper
        output_filename = args.output.name[0]
    except: 
        output_filename = icao+".html"

    print('Output is :', output_filename)

    #Create the data storage
    dict=create_dict_original(filename)
    #pprint(dict)
    
    #check to see if there is a start and end
    try:
        start = args.start[0]
        if debug:
            print('Start is:',args.start[0])
    except:
        start = -1
        if debug:
            print('No start')
    
    try:
        end = args.end[0]
        if debug:
            print('end is:',args.end[0])
    except:
        end = -1
        if debug:
            print('No end')

    
    
    #Now we have the dictionary of what we want to plot. So plot it.
    lats = []
    lons = []
    gmap = gmplot.GoogleMapPlotter(39.0709, -76.60024, 8)
    d_keys = iter(dict.keys()) 
    
    for key in d_keys:
        use = 0
        if start != -1:
            if key < start:
                continue
        if end != -1:
            if key > end:
                continue
 
        td = dict[key]
        if 'lat' in td:
            lats.append(float(td['lat']))
            lons.append(float(td['lon']))
            #print(float(td['lat']),",",float(td['lon']))
            
    gmap.heatmap(lats,lons)
    #gmap.plot(latitudes, longitudes, 'cornflowerblue', edge_width=10)
    #gmap.scatter(more_lats, more_lngs, '#3B0B39', size=40, marker=False)
    #gmap.scatter(marker_lats, marker_lngs, 'k', marker=True)
    #gmap.heatmap(heat_lats, heat_lngs)
    gmap.draw(output_filename)

