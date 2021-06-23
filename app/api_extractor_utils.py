import sys
import os
import json

# date and time
from datetime import datetime, timedelta
from email.utils import parsedate_tz
from dateutil import tz
import time
import config

from api_extractor_config import DATETIME_FORMAT

def load_credentials(access):
    credentials = {}

    if access == 'AgProCanada_TableauDEV':
        credentials = {
            'MSSQL_HOST': config.PYMSSQL_HOST,
            'MSSQL_DB': config.PYMSSQL_DB,
            'MSSQL_USER': config.PYMSSQL_USERNAME,
            'MSSQL_PASS': config.PYMSSQL_PASS,
            'MSSQL_PORT': int(config.PYMSSQL_PORT),
            'MSSQL_DRIVER': config.PYMSSQL_DRIVER
        }
    elif access == 'Youtube_API':
        credentials = config.YOUTUBE_API_CRED
    elif access == 'Youtube_V3_API':
        credentials = config.YOUTUBE_API_V3_CRED
    elif access == 'GA_API':
        credentials = config.GA_API_CRED
    elif access == 'Twitter_API':
        credentials = {
            "consumer_key": config.TWITTER_CONSUMER_KEY,
            "consumer_secret": config.TWITTER_CONSUMER_SECRET,
            "access_token_key": config.TWITTER_ACCESS_TOKEN_KEY,
            "access_token_secret": config.TWITTER_ACCESS_TOKEN_SECRET
        }

    return credentials

def log(s):
    timestamp = datetime.now().strftime(DATETIME_FORMAT)
    print('> [%s]: %s' % (timestamp, s))

def remove_dups(l):
    """Remove duplcates from a list"""
    return list(set(l))

def file_to_str(file_relative_path):
    with open(file_relative_path, 'r') as file:
        return file.read()

def str_to_datetime(datestring):
    """
    String should be RFC822 compliant. Eg. 'Tue Mar 29 08:11:25 +0000 2011'
    Used for twitter API dates
    https://stackoverflow.com/questions/7703865/going-from-twitter-date-to-python-datetime-date
    """
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6]) - timedelta(seconds=time_tuple[-1])
    return dt 

def utc_to_eastern(utc_dt):
    """
    Convert a datetime obejct in UTC to one in Eastern Time Zone
    The utc_dt can be 'naive' (meaning that it does not have tzinfo)
    """
    eastern = tz.gettz('America/Eastern')
    utc_dt = utc_dt.replace(tzinfo=tz.tzutc())
    return utc_dt.astimezone(eastern)

def time_func(func, params):
    """
    Time how long does it take to run a function. 
    """
    t0 = time.time()
    return_val = func(*params)
    t1 = time.time()
    log("'%s' took %.3f seconds to run." % (func.__name__, t1 - t0))
    return  return_val