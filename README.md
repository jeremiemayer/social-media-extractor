# Metrics 

Scripts for fetching analytics metrics from Twitter, YouTube, and Google Analytics.


## Containers
There are two containers which run these scripts. One is twitter_listener, which is always running and listening for the hashtags in the `program_brands` table, and storing them in a SQLite buffer. The other container is nightly_upload, which is run nighltly using the following cronjob:

```
$ crontab -e
30 2 * * *  docker start nightly_upload
```

nightly_upload moves twitter_listener's the buffere to the `twitter_hashtags` table our central SQL datbase. It also updates the general Twitter accounts stats, YouTube analytics, and Google Analytics data.

## Tables
All the tables used in these scripts are in the `imports` schema.

The tables `youtube_channel_stats` anb `twitter_account_stats` have exactly one record added to them every day; showing the general statistcs fot the account on that day.

The `twitter_hashtags` table has a record for every tweet with our hashtags catched by `twitter_listener`.

All of the records in `youtube_videos_stats` and `google_analytics_stats` are deleted completely every day, and replaced by updated data. 


