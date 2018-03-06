import pandas as pd



def ready_frame(df, types='Applicants'):
    
    df = df[df['ga:previousPagePath'].str.contains('jobid')]
    df['jobid'] = df['ga:previousPagePath'].apply(lambda x: x.split('jobid=')[1].split('_')[0])
    df['ga:previousPagePath'] = df['ga:previousPagePath'].apply(lambda x: x.split('&originalsource=')[0])
    df['ga:pagePath'] = df['ga:pagePath'].apply(lambda x: x.split('&originalsource=')[0])
    
    df = df.drop('start_date',axis=1)
    df = df.drop('end_date',axis=1)
    df = df.drop('ga:segment',axis=1)
    
    if types == 'applicants':
        df = df.drop('ga:pagePath',axis=1)
        df.columns = ['Applicants', 'Page','Source', 'JobId']
    elif types == 'views':
        df = df.drop('ga:previousPagePath', axis=1)
        df.columns = ['Page', 'Views', 'Source','JobId']
    else:
        raise ValueError('types: applicants or views only')
    df = df.reset_index().drop('index',axis=1)
   
    return df

def drop_y(df):
    to_drop = [x for x in df if x.endswith('_y')]
    df.drop(to_drop, axis=1, inplace=True)

def rename_x(df):
    for col in df:
        if col.endswith('_x'):
            df.rename(columns={col:col.rstrip('_x')}, inplace=True)

def merge(weekly_views, weekly_applicants):
    weekly = weekly_views.merge(weekly_applicants, on='JobId', how='outer')
    drop_y(weekly)
    rename_x(weekly)
    return weekly

def create_web_pages(weekly):
    """
    weekly: pd.DataFrame with column called Page
    ex. for-job-seekers/search-jobs/?jobid=13241_civil-or-construction-engineering-technologist-3241-ab-calgary-area
    returns: pd.Series
    """
    web_pages = pd.DataFrame(weekly['Page'].unique()).dropna()
    web_pages = 'http://www.theheadhunters.ca' + web_pages
    web_pages.columns = ['Page']
    return web_pages


def ready_recruiter_results(recruiter_results):
    recruiter_results = recruiter_results.groupby([Job,'Source']).sum().fillna(0)
    recruiter_results['Applicants/Views'] = recruiter_results['Applicants']/recruiter_results['Views']
    recruiter_results['Applicants/Views'] = recruiter_results['Applicants/Views'].mul(100).astype(str).add('%')
    return recruiter_results
