import pandas as pd

import report_manager
import report_handler

test = True

analytics = report_manager.initialize_analyticsreporting()
start_date, end_date = report_manager.create_date_range(start_date=None, end_date=None)
weekly_views = report_manager.get_weekly_views(analytics, start_date, end_date)
weekly_applicants = report_manager.get_weekly_applicants(analytics, start_date, end_date)

weekly_applicants.to_csv('weekly_applicants.csv',index=False)
weekly_views.to_csv('weekly_views.csv',index=False)

weekly_applicants = pd.read_csv('weekly_applicants.csv')
weekly_views = pd.read_csv('weekly_views.csv')

weekly_applicants = report_handler.ready_frame(weekly_applicants, types='applicants')
weekly_views = report_handler.ready_frame(weekly_views, types='views')


weekly = report_handler.merge(weekly_views,weekly_applicants)

assert(len(weekly.shape)>0)

web_pages = report_handler.create_web_pages(weekly)
web_pages['Details'] = web_pages['Page'].apply(lambda x: get_stuff_from_url(x))

web_pages.to_csv('web_pages.csv',index=False)

web_pages = pd.read_csv('web_pages.csv')

web_pages[['Name', 'Email', 'Job']] = web_pages['Details'].str.split('|', expand=True)
web_pages = web_pages.drop('Details',axis=1)
web_pages['Page'] = web_pages['Page'].apply(lambda x: x.replace('http://www.theheadhunters.ca', ''))
results = weekly.merge(web_pages, on='Page')
results = results.dropna(subset=['Email'])
results['Page'] = 'http://www.theheadhunters.ca' + results['Page']
results['Views'] = results['Views'].astype(int)
results['Applicants'] = results['Applicants'].fillna(0).astype(int)



names = results['Name'].unique()

subject = email_handler.prepare_subject(start_date, end_date)

for name in names:
	name = 'Danielle Bragge'
	recruiter_results = results[results['Name'] == name]
	first_name = get_first_name(name)
  	emails = recruiter_results['Email'].unique()
	assert(len(emails)==1)
	email = emails[0]
	recruiter_results = report_handler.ready_recruiter_results(recruiter_results)
	html_frame = to_html(recruiter_results)
	email_message = create_message(first_name, html_frame)
  	email = email_handler.prepare_reporting_email(to='joshuabragge@gmail.com', subject=subject, html=email_message)
	email_handler.send_email(email)
	if test:
		break
