import sys
import time

from twitter_stats import update_twitter_metrics, stylish_print
from youtube_stats import update_youtube_metrics
from google_analytics import update_ga_metrics
from upload_twitter_stream_buffer import upload_stream_buffer

import results_generator as rg

from api_extractor_config import DEBUG
from api_extractor_utils import file_to_str, log

NUMBER_OF_TRIES = 3
COOLDOWN_TIME = 5 * 1 # in seconds

# DEBUG mode specific config
if DEBUG:
    COOLDOWN_TIME = 1

def try_to_fetch(fetch_stat_func, mssql_engine):
    """Try to fetch multiple times, so we can avoid temporary network problems."""
    succeeded = False
    tries = 0
    log('First try...')
    while not succeeded and tries < NUMBER_OF_TRIES - 1:
        try:
            tries += 1
            fetch_stat_func(mssql_engine)

            # in DEBUG mode, fail the first time for testing purposes
            if DEBUG and tries <= 2:
                raise Exception('We fail the first few times for testing purposes.')

            succeeded = True
        except Exception as ex:
            e = str(ex)
            log('Exceptin occured: %s' % e)
            log('Number of tries so far: %s/%s' % (tries, NUMBER_OF_TRIES))
            time.sleep(COOLDOWN_TIME)
    if not succeeded:
        tries += 1
        log('Final try (%s/%s)' % (tries, NUMBER_OF_TRIES))
        fetch_stat_func(mssql_engine)

def main(mssql_engine):
    stylish_print('Youtube Metrics')
    try_to_fetch(update_youtube_metrics, mssql_engine)

    #stylish_print('Twitter Metrics')
    #try_to_fetch(update_twitter_metrics, mssql_engine)

    #stylish_print('Google Analytics Metrics')
    #try_to_fetch(update_ga_metrics, mssql_engine)

    #stylish_print('Twitter Hashtags')
    #try_to_fetch(upload_stream_buffer, mssql_engine)

if __name__ == '__main__':
    mssql_engine = rg.connect_to_db()
    main(mssql_engine)
 
            

