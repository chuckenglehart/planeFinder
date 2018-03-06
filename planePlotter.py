#!/usr/bin/env python3
""" Written to target Python3
Requires gmplot to be installed.
"""

import argparse
from pprint import pprint # for pretty dictionary printing.
import gmplot
import planeUtil as pUtil
import planeDatabase as pd

def main():
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
    dbc = pd.DBConnection()

    ref_lon = -76.600144
    ref_lat = 39.070902

    if dbc.get_connection_isopen():
        print("Database connection open")
    else:
        dbc = -1
        print("Database connection not open")

    #filename="data/"+icao+".txt"

    try:
        #get the name from an io txt wrapper
        output_filename = args.output.name[0]
    except:
        output_filename = icao+".html"

    print('Output is :', output_filename)

    #Create the data storage
    data_dict = dbc.get_dict(icao)
    #dataList=dbc.get_dict(icao)
    pprint(data_dict)

    #check to see if there is a start and end
    try:
        start = args.start[0]
        if debug:
            print('Start is:', args.start[0])
    except:
        start = -1
        if debug:
            print('No start')

    try:
        end = args.end[0]
        if debug:
            print('end is:', args.end[0])
    except:
        end = -1
        if debug:
            print('No end')

    #Now we have the dictionary of what we want to plot. So plot it.
    lats = []
    lons = []
    gmap = gmplot.GoogleMapPlotter(ref_lat, ref_lon, 8)

    """
    for row in dataList:
        pprint(row['time'])
        if start != -1:
            if row['time'] < start:
                continue
        if end != -1:
            if row['time'] > end:
                continue

        if 'lat' in row and type(row['lat']) is float:
            lats.append(float(row['lat']))
            lons.append(float(row['lon']))
            #print(float(td['lat']),",",float(td['lon']))
    """
    #need to rewrite the get dict to make this work
    #temportarily using the above to amek a list work

    d_keys = iter(data_dict.keys())

    for key in d_keys:
        if start != -1:
            if key < start:
                continue
        if end != -1:
            if key > end:
                continue

        td = data_dict[key]
        if 'lat' in td and type(td['lat']) is float:
            lats.append(float(td['lat']))
            lons.append(float(td['lon']))
            #print(float(td['lat']),",",float(td['lon']))

    gmap.heatmap(lats, lons)
    #gmap.plot(latitudes, longitudes, 'cornflowerblue', edge_width=10)
    #gmap.scatter(more_lats, more_lngs, '#3B0B39', size=40, marker=False)
    #gmap.scatter(marker_lats, marker_lngs, 'k', marker=True)
    #gmap.heatmap(heat_lats, heat_lngs)
    gmap.draw(output_filename)

if __name__ == '__main__':
    main()
