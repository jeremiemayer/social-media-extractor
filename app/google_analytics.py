"""Hello Analytics Reporting API V4."""

import argparse

from googleapiclient.discovery import build
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client.client import flow_from_clientsecrets, Credentials
from oauth2client import tools

from pprint import pprint

import results_generator as rg
from sqlalchemy import *

from api_extractor_utils import *
from api_extractor_config import *
from datetime import datetime
import time

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
VIEW_ID = '10490692'
SCRIPT_RUN_TIME = datetime.now().strftime(DATETIME_FORMAT)

# Please make sure that Google Analytics API is enabled for the project.

def get_authenticated_service():
    """Initializes the analyticsreporting service object.

    Returns:
        analytics an authorized analyticsreporting service object.
    """

    json = load_credentials('GA_API')
    credentials = Credentials.new_from_json(json)
    http = credentials.authorize(http=httplib2.Http())

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', http=http)

    return analytics


def get_report(analytics, page_token=None):
    """Queries the Analytics Reporting API V4."""

    def make_req(page_token=None):
        rep_req = {
            'viewId': VIEW_ID,
            # TODO(mkeyhani): set the start date properly
            'dateRanges': [{'startDate': '2017-01-01', 'endDate': 'today'}],
            'metrics': [{'expression': 'ga:uniquePageviews'}],
            'dimensions': [
                {'name': 'ga:hostname'},
                {'name': 'ga:pagePath'},
                {'name': 'ga:pageTitle'},
                {'name': 'ga:landingPagePath'},
                {'name': 'ga:medium'},
                {'name': 'ga:date'},
                {'name': 'ga:source'}
            ],
            'pageSize': '1000' if DEBUG else '10000'
        }

        if page_token:
            rep_req['pageToken'] = page_token

        return { 'reportRequests': [rep_req] }


    current_page = analytics.reports().batchGet(body=make_req(page_token)).execute()['reports'][0]
    aggregate = current_page['data']['rows']

    while "nextPageToken" in current_page and (not DEBUG or len(aggregate) < 3000): #
        current_page = analytics.reports().batchGet(body=make_req(current_page ['nextPageToken'])).execute()['reports'][0]
        aggregate.extend(current_page['data']['rows'])


    column_header = current_page.get('columnHeader', {})
    dimension_headers = column_header.get('dimensions', [])
    metric_headers = column_header.get('metricHeader', {}).get('metricHeaderEntries', [])

    return (dimension_headers, metric_headers, aggregate)

def get_ga_stats(ga_api):
    (dimension_headers, metric_headers, report) = time_func(get_report, [ga_api])

    records = []
    for row in report:
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        record = {}

        for header, dimension in zip(dimension_headers, dimensions):
            record[header] = dimension

        for i, values in enumerate(dateRangeValues):
            for metric_header, value in zip(metric_headers, values.get('values')):
                record[metric_header.get('name')] = value

        record['Extract_Datetime'] = SCRIPT_RUN_TIME
        records.append(record)

    log('%s records fetched from Google Analytics' % len(records))
    return records


def update_ga_metrics(mssql_engine):
    ga_api = get_authenticated_service()

    ga_stats = get_ga_stats(ga_api)

    rg.update_db(ga_stats,
        'google_analytics_stats',
        mssql_engine,
        drop=(not DEBUG) # drop the table, but not while debugging
    )
        # We may not need a schema since we are dropping a table evert time
        #schema=ga_table_schema)


if __name__ == "__main__":
    mssql_engine = rg.connect_to_db()
    update_ga_metrics(mssql_engine)
