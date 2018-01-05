# planeFinder
Python program that reads in raw ADS-B and filters based on ICAO number. 
I have found it easiest to connect to a raw tcp dump coming fom 
an instance of dump1090

This program requires pyModeS which can be found at:
https://github.com/junzis/pyModeS

Currently in development, this should be taken as alpha at best.  

Arguments: 

-c <config file location>

-i <ipaddress (not a hostname)>

-p <port>

-s  Silent mode - No printf 

-f  Write decoded information to file

-d  Write decoded information to database

python planefinder.py -i <IPAddress> -p <PORT> -c <FILENAME> -s -f -d 

