/* Twitter statuses reconciliation */
select count(*), sum(favourite_count) 
from [imports].[twitter_statuses]
where [Extract_Datetime] > '2018-05-13' and [Created_date] between '2017-10-01' and '2018-05-12'

select count(*), sum(favourite_count) 
from [imports].[twitter_statuses]
where [Extract_Datetime] > '2018-05-13' and [Created_date] between '2017-10-01' and '2018-05-12'

/* Youtube videos recon. */
select count(*),sum(views)
from [imports].[youtube_videos_stats]
where [Extract_Datetime] > '2018-05-13'

select count(*),sum(views)
from [imports].[youtube_videos_stats]
where [Extract_Datetime] > '2018-05-13'

/* Twitter account stast reconciliation */
select top 1 * from [imports].[twitter_account_stats] order by extract_datetime desc
select top 1 * from [imports].[twitter_account_stats] order by extract_datetime desc

/*Youtube Channel stats recon. */
select top 1 * from [imports].[youtube_channel_stats] order by extract_datetime desc
select top 1 * from [imports].[youtube_channel_stats] order by extract_datetime desc

/* Google Analytics reconciliation */
select count(*), sum(cast([ga:uniquePageViews] as int))
from [imports].[google_analytics_stats] 
where [Extract_Datetime] > '2018-05-13'

select count(*), sum(cast([ga:uniquePageViews] as int))
from [imports].[google_analytics_stats] 
where [Extract_Datetime] > '2018-05-13'








