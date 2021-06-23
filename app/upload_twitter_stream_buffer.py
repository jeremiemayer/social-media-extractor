from sqlalchemy import *
from datetime import datetime, timedelta
from api_extractor_config import *
import results_generator as rg

SCRIPT_RUN_TIME = datetime.now().strftime(DATETIME_FORMAT)

def upload_stream_buffer(mssql_eng):
    column_names = [column[0] for column in twitter_hashtags_sqlite_schema]

    sqlite_eng = create_engine("sqlite:///%s/%s.db" % (DATA_FOLDER, HASHTAGS_BUFFER_DB))
    sqlite_conn = sqlite_eng.connect()

    last_day = (datetime.now() - timedelta(days=1)).date().strftime("%Y_%m_%d")
    table_name = '%s_%s' % (HASHTAGS_BUFFER_DB, last_day)

    if (table_name in sqlite_eng.engine.table_names()):
        query =  "select * from [%(table)s]" % {'table': table_name}
        rows = sqlite_conn.execute(query)

        tweets = []
        for row in rows:
            tweet = dict(zip(column_names, row))
            tweet['Extract_Datetime'] = SCRIPT_RUN_TIME
            tweets.append(tweet)
        
        rg.update_db(tweets,
            'twitter_hashtags',
            mssql_eng,
            schema=twitter_hashtags_schema())

if __name__ == '__main__':
    mssql_eng = rg.connect_to_db()
    upload_stream_buffer(mssql_eng)


    