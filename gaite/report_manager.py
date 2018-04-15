import os

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import datetime
import pandas as pd
import time


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.path.join('secrets', 'client_secrets.json')
VIEW_ID = '35662697'


def initialize_analyticsreporting():
    """Initializes an Analytics Reporting API V4 service object.

    Returns:
    An authorized Analytics Reporting API V4 service object.
    """
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
    KEY_FILE_LOCATION, SCOPES)

    # Build the service object.
    analytics = build('analyticsreporting', 'v4', credentials=credentials)

    return analytics


def get_applications_report(analytics, start_date='7daysAgo', end_date='today'):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
          'metrics': [{'expression': 'ga:pageviews'}],
          'dimensions': [{'name': 'ga:previousPagePath'}, 
                         {'name': 'ga:pagePath'}, 
                         {'name' : 'ga:source'}, 
                         {"name": "ga:segment"},
                        ],
    "segments":[
    {
      "dynamicSegment":
      {
        "name": "ApplicationSubbmited",
        "userSegment":
        {
          "segmentFilters":[
          {
            "simpleSegment":
            {
              "orFiltersForSegment":
              {
                "segmentFilterClauses": [
                {
                  "dimensionFilter":
                  {
                    "dimensionName":"ga:pagePath",
                    "operator":"PARTIAL",
                    "expressions":["/application-submitted"]
                  }
                }]
              }
            }
          }]
        }
      }
    }
    ]
        }]
      }
  ).execute()

def get_views_report(analytics, start_date='7daysAgo', end_date='today'):
      """Queries the Analytics Reporting API V4.

      Args:
        analytics: An authorized Analytics Reporting API V4 service object.
      Returns:
        The Analytics Reporting API V4 response.
      """
      return analytics.reports().batchGet(
          body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
          'metrics': [{'expression': 'ga:pageviews'}],
          'dimensions': [{'name': 'ga:previousPagePath'},
                         {'name': 'ga:pagePath'},
                         {'name' : 'ga:source'},
                         {"name": "ga:segment"},
                        ],
        "segments":[
        {
          "dynamicSegment":
          {
        "name": "ApplicationSubbmited",
        "userSegment":
        {
          "segmentFilters":[
          {
            "simpleSegment":
            {
              "orFiltersForSegment":
              {
                "segmentFilterClauses": [
                {
                  "dimensionFilter":
                  {
                    "dimensionName":"ga:pagePath",
                    "operator":"PARTIAL",
                    "expressions":["jobid="]
                  }
                }]
              }
            }
          }]
        }
          }
        }
        ]
        }]
          }
      ).execute()


def response_to_frame(response, start_date='2018-02-01', end_date='2018-02-02'):
    list = []
    # get report data
    for report in response.get('reports', []):
    # set column headers
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])

        for row in rows:
            # create dict for each row
            dict = {}
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            # fill dict with dimension header (key) and dimension value (value)
            for header, dimension in zip(dimensionHeaders, dimensions):
                dict[header] = dimension

            # fill dict with metric header (key) and metric value (value)
            for i, values in enumerate(dateRangeValues):
                for metric, value in zip(metricHeaders, values.get('values')):
                    # set int as int, float a float
                    if ',' in value or ',' in value:
                        dict[metric.get('name')] = float(value)
                    else:
                        dict[metric.get('name')] = int(value)

            list.append(dict)

    df = pd.DataFrame(list)
    df['start_date'] = start_date
    df['end_date'] = end_date
    return df


def get_weekly_views(analytics, start_date, end_date):
    """
    start_date: datetime
    end_date: datetime
    """
    initial_date = start_date
    delta = end_date - initial_date
    days_between = delta.days
    
    df = pd.DataFrame()

    for day in range(days_between):
        report_date = initial_date + datetime.timedelta(days=day)
        report_date = report_date.strftime('%Y-%m-%d')

        end_date = report_date
        start_date = report_date

        response = get_views_report(analytics, start_date=start_date, end_date=end_date)
        tf = response_to_frame(response, start_date=start_date, end_date=end_date)
        
        
        try:
            tf = tf[tf['ga:pagePath'].str.contains('jobid=')]
        except:
            # print('Error grabbing vies for range:', start_date, end_date)
            time.sleep(1)
            continue  # exit loop
            
        df = pd.concat([df, tf])

        # print('Grabbing views for range:', start_date, end_date)
        time.sleep(1)
        
    return df
        
def get_weekly_applicants(analytics, start_date, end_date):
    """
    start_date: datetime
    end_date: datetime
    """
    initial_date = start_date
    delta = end_date - start_date
    days_between = delta.days
    
    df = pd.DataFrame()

    for day in range(days_between):
        report_date = initial_date + datetime.timedelta(days=day)
        report_date = report_date.strftime('%Y-%m-%d')

        end_date = report_date
        start_date = report_date

        response = get_applications_report(analytics, start_date=start_date, end_date=end_date)
        tf = response_to_frame(response, start_date=start_date, end_date=end_date)
        
        try:
            tf = tf[tf['ga:pagePath'].str.contains('/application-submitted')]
        except KeyError:
            print('Error grabbing applicants for range:', start_date, end_date) 
            time.sleep(1)
            continue # break loop
        
        df = pd.concat([df,tf])

        print('Grabbing applicants for range:', start_date, end_date)
        time.sleep(1)
        
    return df


def create_date_range(start_date=None, end_date=None):
    """
    :param start_date:  str date in '%Y-%m-%d' or None if td - 7 d
    :param end_date: str date in '%Y-%m-%d' or None if TD
    :return: str datetime start_date, end_date
    """
    if end_date is None:
        end_date = datetime.datetime.now()
        finish_date = end_date
    else:
        finish_date = end_date
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
    if start_date is None:
        start_date = finish_date - datetime.timedelta(weeks=1)
    else:
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    
    return start_date, end_date
