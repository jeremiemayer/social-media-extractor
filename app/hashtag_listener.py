#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from api_extractor_utils import load_credentials, remove_dups, log, utc_to_eastern
from api_extractor_config import *

from sqlalchemy import create_engine
import results_generator as rg

from datetime import datetime, timedelta
from pprint import pprint
import os

#-----------------------------------------------------------------------
# configs
#-----------------------------------------------------------------------
BUFFER_DB_NAME = 'hashtags'


def format_hashtags(hashtags):
    if hashtags is not None:
        return ';'.join(remove_dups('#'+h['text'] for h in hashtags))

class HashtagListener(StreamListener):
    def __init__(self):
        super(HashtagListener, self).__init__()
        print('listener initialized')
        self.db_eng = create_engine("sqlite:///%s/%s.db" % (DATA_FOLDER, HASHTAGS_BUFFER_DB), 
                connect_args={'check_same_thread': False})
        self.db_conn = self.db_eng.connect()

    def on_status(self, status):
        def make_record(tweet):
            def get_full_text(tweet):
                """ If the tweet is too long, the API only returns 140 chars in
                'text' for compatibity reasons. See https://developer.twitter.com/en/docs/tweets/tweet-updates.html
                
                The 'text' field is also truncated for retweets.

                So, to get the full text, we need to do some gymnastics.
                """
                if hasattr(tweet, 'retweeted_status'):
                    return get_full_text(tweet.retweeted_status)
                elif hasattr(tweet, 'extended_tweet'):
                    return tweet.extended_tweet['full_text']
                else:
                    return tweet.text
            def get_hashtags(tweet):
                if hasattr(tweet, 'quoted_status'):
                    get_hashtags(tweet.quoted_status)
                else:
                    if hasattr(tweet,'entities'):
                        return tweet.entities.get('hashtags')
                    else:
                        return []
                
            record = {
                'TweetID': tweet.id,
                'TweetText': get_full_text(tweet),
                'AccountID': tweet.user.id,
                'DisplayName': tweet.user.name,
                'Handle': tweet.user.screen_name,
                'CreatedAt':  utc_to_eastern(tweet.created_at).strftime(DATETIME_FORMAT),
                'Hashtags': format_hashtags(get_hashtags(tweet))
            }

            return record

        schema = twitter_hashtags_sqlite_schema
        column_names = [column[0] for column in schema]

        today = (datetime.now()).strftime("%Y_%m_%d") 
        table_name = '%s_%s' % (HASHTAGS_BUFFER_DB, today)

        if not (table_name in self.db_eng.engine.table_names()):
            # TODO(mkeyhani): delete the old tables (maybe in the script which pushes these to the central DB)
            
            query_parts = {
                'table_name': table_name,
                'schema': ','.join([(column[0] + " " + column[1]) for column in schema])
            }
            query = "create table %(table_name)s (%(schema)s)" % query_parts
            res = self.db_conn.execute(query)
            if DEBUG:
                log("Table '%s' created." % table_name)


        record = make_record(status)
        if DEBUG:
            print(dir(status))
        
        #pprint(record)

        query_parts = {
            'table_name': table_name,
            'column_list':  ','.join(column_names),
            'params': ','.join([(":"+column) for column in column_names])
        }
        query =  "insert into %(table_name)s (%(column_list)s) values (%(params)s)" % query_parts
        self.db_conn.execute(query, **record)
        if DEBUG:
            print(query)

    def on_error(self, err):
        print(err)
        os._exit(1)


def start_listening():
    cred = load_credentials('Twitter_API')

    auth = OAuthHandler(cred['consumer_key'], cred['consumer_secret'])
    auth.set_access_token(cred['access_token_key'], cred['access_token_secret'])

    listener = HashtagListener()
    stream = Stream(auth, listener) #  authetification and the connection to Twitter Streaming API

    mssql_engine = rg.connect_to_db()
    rows = rg.run_sql_script(mssql_engine, 'select distinct Brand, Hashtag from %s.BrandData' % SCHEMA_NAME)

    hashtags = []
    for row in rows:
        if row[1] != None:
            hashtags.append(row[1])
    #print(hashtags)
    if DEBUG:
       print('Listening for %s' % hashtags)

    stream.filter(track=hashtags, async=True)

if __name__ == '__main__':
    start_listening()



