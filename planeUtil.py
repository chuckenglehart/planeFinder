"""
Utility functions that are used often

"""
import time
from datetime import datetime

#Quick function to see if a character is a hex
def ishex(string):
    """
    Function to check to see if the string only contains hex. This is a
        pure hex checker. 0x00 returns False, but 00 would return True.
    Args:
        string (str): String to be checked to see if it is hex

    Returns:
        bool: True if string only contains hex, False otherwise
    """
    hex_str = ['a', 'b', 'c', 'd', 'e', 'f', 'A', 'B', 'C', 'D', 'E', 'F',
               '0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    if len(string) < 1:
        return False
    for i in string:
        if hex_str.count(i) != 1:
            return False
    return True

def custom_time_decode(string):
    """
    Some words about checking the custom time
    Args:
        string (str): String of the custom time format YYYYMMDDTHH:MM:SS

    Returns:
        time_struct: contains the converted time
    """
    #convert input string to Time
    t = time.strptime(string, "%Y%m%dT%H:%M:%S")
    #TODO: check to see something about milliseconds
    if False: #check to see if it is a correct string
        msg = "%s is not in the correct time format of YYYYMMDDTHH:MM:SS" % string
        raise msg
    return t
