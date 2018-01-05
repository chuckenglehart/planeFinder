"""
Utility functions that are used often

"""
import time
#from time import mktime
from datetime import datetime

#Quick function to see if a character is a hex
def ishex(string):
    hex=['a','b','c','d','e','f','A','B','C','D','E','F','0','1','2','3','4','5','6','7','8','9']
    if len(string) < 1:
        return False
    for i in string:
        if hex.count(i) != 1:
            return False
    return True

def custom_time_decode(string):
    #convert input string to Time
    t = time.strptime(string,"%Y%m%dT%H:%M:%S")
    #TODO: check to see something about milliseconds
    if False: #check to see if it is a correct string
        msg = "%s is not in the correct time format of YYYYMMDDTHH:MM:SS" % string
        raise argparse.ArgumentTypeError(msg)
    return t
