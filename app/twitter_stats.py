#!/usr/bin/python
import sys
import os
import json
from os import getenv, path

# date and time
from datetime import datetime, timedelta
from email.utils import parsedate_tz

# for testing
import pprint as pp
from pprint import pprint
import random
from progress.spinner import Spinner

from twitter import *
import results_generator as rg

from api_extractor_utils import load_credentials, log, str_to_datetime, utc_to_eastern
from api_extractor_config import DEBUG, CREATE_TABLES, DATE_FORMAT, DATETIME_FORMAT, twitter_account_schema, twitter_status_schema

from sqlalchemy import *

#-----------------------------------------------------------------------
# Config
#-----------------------------------------------------------------------
SCRIPT_RUN_TIME = datetime.now().strftime(DATETIME_FORMAT)


#-----------------------------------------------------------------------
# Helper Functions
#-----------------------------------------------------------------------
def remove_dups(lst):
    return list(set(lst))

def get_formatted_hashtags(tweet):
    """Return the hashtags in form '#hash1;#hash2' """
    return ';'.join(get_hashtags_list(tweet))

def get_hashtags_list(tweet):
    """Tweet should be a dict"""
    return remove_dups(
        ['#' + hashtag['text'] for hashtag in tweet['hashtags']]
    )

def stylish_print(s):
    print('=='*len(s) + '==')
    print("> " + s)
    print('=='*len(s) + '==')

def underlined_print(s):
    print(s)
    print('-' * 80)

def paginate(req, per_page=200, **params):
    """Fetch as much data as possible using the specified request.

    This is useful because some request have a limit on the number of items per
    request. Thus, we'll need to paginate untill we have fetched as much as
    possible.

    This function is specific to twitter. There is a seperate one for Youtube.
    There are minor but essential differences.
    """

    # When debugging, we fetch 3 items per request to test pagination.
    # Also we only fetch (about) 10 in total to avoid waiting too much ;)
    if DEBUG:
        per_page = 3

    current_page =  req(count=per_page, **params)
    aggregate = current_page
    while len(current_page) > 0 and (not DEBUG or len(aggregate) < 10):
        oldest_in_page = current_page[-1].id
        current_page = req(count=per_page, max_id=oldest_in_page - 1, **params)
        aggregate.extend(current_page)

    return aggregate

def to_datetime(datestring):
    time_tuple = parsedate_tz(datestring.strip())
    dt = datetime(*time_tuple[:6])
    return dt - timedelta(seconds=time_tuple[-1])

#-----------------------------------------------------------------------
# General Account Statistics
#-----------------------------------------------------------------------
def get_account_stats(acc_info):
    def get_account_mentions():
        """ API limitaion: this obly fetches up 800 mentions """
        mentions = paginate(api.GetMentions, per_page=200)  # 20 is the max the API provides
        return mentions

    def pick_new_mentions(mentions):
        def is_new(mention):
            now = datetime.now()

            yesterday = (now - timedelta(days=1)).date()
            mention_date = utc_to_eastern(str_to_datetime(mention.AsDict()['created_at'])).date()

            # TODO(mkyhani): test this extensively
            return mention_date == yesterday

        return list(filter(is_new, mentions))

    def get_new_mentions_count():
        return len(pick_new_mentions(get_account_mentions()))

    record = {
        'Extract_Datetime' : SCRIPT_RUN_TIME, 
        'Account_ID': acc_info['id'],
        'Display_Name': acc_info['name'],
        'Handle': acc_info['screen_name'],
        'Description': acc_info['description'],
        'Favourites_Count': acc_info['favourites_count'],
        'Followers_Count': acc_info['followers_count'],
        'Friends_Count': acc_info['friends_count'],
        'Listed_Count': acc_info.get('listed_count', '0'),
        'Statuses_Count': acc_info['statuses_count'],
        'Daily_Mentions_Count': get_new_mentions_count(),
        'Profile_URL': 'https://twitter.com/' + acc_info['screen_name'],
        'Profile_Image': acc_info.get('profile_image_url', ''),
        'Banner_Image': acc_info.get('profile_banner_url', ''),
        'Location': acc_info['location'],
    }

    return [record]

#-----------------------------------------------------------------------
# Tweet Statistics
#-----------------------------------------------------------------------
def get_tweet_metrics():
    def make_record(tweet):

        # convert timzone and format properly
        created_at = utc_to_eastern(str_to_datetime(tweet['created_at'])) 
        created_at = created_at.strftime(DATETIME_FORMAT)

        return {
            'Tweet_ID': tweet['id_str'],
            'Tweet_Text': tweet['full_text'], # TODO(mkeyhani): truncate this if it's too long
            'Favourite_Count': tweet.get('favorite_count', '0'), # TODO(mkeyhani) make sure defaulting is the right thing to do
            'Retweet_Count': tweet.get('retweet_count', '0'),
            'Hashtags': get_formatted_hashtags(tweet),
            'Handle': tweet.get('user', {}).get('screen_name', ''),
            'URLs': tweet.get('urls', []),
            'User_Mentions': tweet.get('user_mentions', []),
            'Media': tweet.get('media', []),
            'Place': tweet.get('place', []),
            'Favorited': tweet.get('favorited', False),
            'Retweeted': tweet.get('retweeted', False),
            'In_reply_to_screen_name': tweet.get('in_reply_to_screen_name', ''),

            'Extract_Datetime' : SCRIPT_RUN_TIME,
            'Created_Date': created_at,
        }

    log('Fetching tweets...')
    params = { 'screen_name': acc_info['screen_name'] }
    tweets = paginate(api.GetUserTimeline, per_page=200, **params)

    records =[]
    for tweet in tweets:
        records.append(make_record(tweet.AsDict()))
   
    return records


def update_twitter_metrics(mssql_engine):
    global api, acc_info

    log('Authentication and fetching twitter account stats...')
    api = Api(tweet_mode='extended', **load_credentials('Twitter_API')) # load our API credentials
    acc_info = api.VerifyCredentials().AsDict() # authenticate and get account information

    account_stats = get_account_stats(acc_info)
    tweet_metrics = get_tweet_metrics()

    rg.update_db(account_stats,
        'twitter_account_stats',
        mssql_engine,
        schema=twitter_account_schema())
    rg.update_db(tweet_metrics,
        'twitter_statuses',
        mssql_engine,
        drop=False,
        schema=twitter_status_schema())


if __name__ == "__main__":
    mssql_engine = rg.connect_to_db()
    update_twitter_metrics(mssql_engine)

