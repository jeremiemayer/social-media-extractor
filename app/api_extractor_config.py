from sqlalchemy import *

DEBUG = False

# ***************************************************************************
# > BE VERY CAREFUL ABOUT CREATE_TABLES.
# > WHEN SET TO TRUE IT WILL DROP ALL THE EXISTING TABLES AND CREATES NEW ONES.
# ***************************************************************************
CREATE_TABLES = False

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

HASHTAGS_BUFFER_DB = 'hashtags'
DATA_FOLDER = 'persistent_data'

SCHEMA_NAME = 'imports'


#-----------------------------------------------------------------------
# NOTE ON SCHEMAS:
#    The schemas are wrapped inside a function, so that every time the function
#    is called we get a new set of Column objects.
#    
#    This is needed for the 'retry a few times' feature, when we create the
#    tableclass multiple times. I am not exactly sure why.
#-----------------------------------------------------------------------

#-----------------------------------------------------------------------
# Twitter Table Schemas
#-----------------------------------------------------------------------
def twitter_account_schema():
    twitter_account_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'Account_ID': Column(String(length=30)),
        'Display_Name': Column(String(length=100)),
        'Handle': Column(String(length=30)),
        'Statuses_Count': Column(Integer()),
        'Followers_Count': Column(Integer()), 
        'Friends_Count': Column(Integer()),
        'Favourites_Count': Column(Integer()),
        'Listed_Count': Column(Integer()),
        'Profile_URL': Column(String(length=300)),
        'Profile_Image': Column(String(length=300)),
        'Banner_Image': Column(String(length=300)),
        'Location': Column(String(length=100)),
        'Timezone': Column(String(length=50)),
        'Description': Column(String(length=400)),
        'Daily_Mentions_Count': Column(Integer()),
        'Autogen_ID': Column(Integer(), primary_key=True),
        '__tablename__': 'twitter_account_stats',
        '__table_args__': {'schema' : SCHEMA_NAME}
    }

    return twitter_account_schema

def twitter_status_schema():
    twitter_status_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'Tweet_ID': Column(String(length=30)),
        'Created_Date': Column(String(length=30)),
        'Handle': Column(String(length=30)),
        'Tweet_Text': Column(String(length=700)),
        'Favourite_Count': Column(Integer()),
        'Retweet_Count': Column(Integer()),
        'Hashtags': Column(String(length=100)),
        'Media': Column(String(length=3000)),
        'URLs': Column(String(length=500)),
        'Place': Column(String(length=500)),
        'User_Mentions': Column(String(length=5000)),
        'Favorited': Column(String(length=20)),
        'Retweeted': Column(String(length=20)),
        'In_reply_to_screen_name': Column(String(length=30)),
        'Autogen_ID': Column(Integer(), primary_key=True),
        '__tablename__': 'twitter_statuses',
        '__table_args__': {'schema' : SCHEMA_NAME}
    }

    return twitter_status_schema

# For the mssql table
def twitter_hashtags_schema():
    twitter_hashtags_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'TweetID': Column(String(length=30)),
        'TweetText': Column(String(length=700)),
        'AccountID': Column(String(length=30)),
        'DisplayName': Column(String(length=100)),
        'Handle': Column(String(length=30)),
        'CreatedAt': Column(String(length=30)),
        'Hashtags': Column(String(length=100)),
        'Autogen_ID': Column(Integer(), primary_key=True),
        '__tablename__': 'twitter_hashtags',
        '__table_args__': {'schema' : SCHEMA_NAME}
    }

    return twitter_hashtags_schema

# For the sqlite buffer table
twitter_hashtags_sqlite_schema = [
    ('TweetID', 'text'),
    ('TweetText', 'text'),
    ('AccountID', 'text'),
    ('DisplayName', 'text'),
    ('Handle', 'text'),
    ('CreatedAt', 'text'),
    ('Hashtags', 'text')
]

#-----------------------------------------------------------------------
# Youtube  Table Schemas
#-----------------------------------------------------------------------
def channel_table_schema():
    channel_table_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'channel_name': Column(String(length=200)),
        'channel_id': Column(String(length=30)),
        'total_comments': Column(String(length=30)),
        'subscribers': Column(Integer()),
        'new_subscribers': Column(Integer()),
        'total_views': Column(String(length=30)),
        'videos': Column(Integer()),
        'Autogen_ID': Column(Integer(), primary_key=True), # add a PK - SQLAlchemy demands this
        '__tablename__': 'youtube_channel_stats', #tablename assignment is done inside the dictionary
        '__table_args__': {'schema' : SCHEMA_NAME}
    }

    return channel_table_schema


def videos_table_schema():
    videos_table_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'publishedAt': Column(String(length=30)),
        'video': Column(String(length=50)),
        'title': Column(String(length=200)),
        'likes': Column(Integer()), 
        'dislikes': Column(Integer()),
        'comments': Column(Integer()),
        'shares': Column(Integer()),
        'views': Column(Integer),
        'averageViewDuration': Column(Integer),
        'subscribersGained': Column(Integer),
        'subscribersLost': Column(Integer),
        'Autogen_ID': Column(Integer(), primary_key=True), # add a PK - SQLAlchemy demands this
        '__tablename__': 'youtube_videos_stats', #tablename assignment is done inside the dictionary
        '__table_args__': {'schema' : SCHEMA_NAME} 
    }

    return videos_table_schema


def videos_tags_schema():
    videos_table_schema = {
        'Extract_Datetime': Column(String(length=30)),
        'video_id': Column(String(length=50)),
        'video_tag': Column(String(length=50)),
        'Autogen_ID': Column(Integer(), primary_key=True), # add a PK - SQLAlchemy demands this
        '__tablename__': 'youtube_videos_tags', #tablename assignment is done inside the dictionary
        '__table_args__': {'schema' : SCHEMA_NAME} 
    }

    return videos_table_schema