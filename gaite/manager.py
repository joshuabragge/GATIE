
from ruamel.yaml import YAML

from gaite import email_handler, web_handler, report_manager, report_handler, environment


class ProcessManager(Env):
    def __init__(self):
        yaml = YAML()
        self.config = yaml.load('../runtime.yaml')
        self.env = self._shared_params['env']

        if self._shared_params['recruiter'] is None:
            self.single_recruiter = self.config[self.env]['recruiter']
        else:
            self.single_recruiter = self._shared_params['recruiter']

        self.start_date = None
        self.end_date = None
        self.weekly_applicants = None
        self.weekly_views = None
        self.web_pages = None
        self.results = None
        self.names = None
        self.subject = None

    def run(self):

        self.get_analytics_reports()
        if self.save:
            self.save_analytics_reports()

        self.clean_reports()
        self.transform_reports_for_web_query()

        if self.save:
            self.save_web_report()

        self.parse_web_pages()

        if self.save:
            self.save_web_report()

        self.add_emails_to_reports()

        if self.save:
            self.self.save_complete_reports()

        self.prepare_emails()

        self.send_emails()

    def get_analytics_reports(self):
        analytics = report_manager.initialize_analyticsreporting()
        self.start_date, self.end_date = report_manager.create_date_range(start_date=None, end_date=None)
        self.weekly_views = report_manager.get_weekly_views(analytics, start_date, end_date)
        self.weekly_applicants = report_manager.get_weekly_applicants(analytics, start_date, end_date)

    def save_analytics(self):
        self.weekly_applicants.to_csv('weekly_applicants.csv',index=False)
        self.weekly_views.to_csv('weekly_views.csv',index=False)

    def clean_reports(self):
        weekly_applicants = report_handler.ready_frame(self.weekly_applicants, types='applicants')
        weekly_views = report_handler.ready_frame(self.weekly_views, types='views')
        weekly = report_handler.merge(weekly_views, weekly_applicants)
        assert(len(weekly.shape) > 0)
        self.web_pages = report_handler.create_web_pages(weekly)

    def transform_reports_for_web_query(self):
        self.web_pages['Details'] = self.web_pages['Page'].apply(lambda x: web_handler.get_stuff_from_url(x))
        self.web_pages[['Name', 'Email', 'Job']] = self.web_pages['Details'].str.split('|', expand=True)
        self.web_pages = web_pages.drop('Details', axis=1)

    def save_web_report(self):
        self.web_pages.to_csv('web_pages.csv', index=False)

    def parse_web_pages(self):
        self.web_pages['Page'] = self.web_pages['Page'].apply(lambda x: x.replace('http://www.theheadhunters.ca', ''))

    def add_emails_to_reports(self):
        results = self.weekly.merge(web_pages, on='Page')
        results = results.dropna(subset=['Email'])
        results['Page'] = 'http://www.theheadhunters.ca' + results['Page']
        results['Views'] = results['Views'].astype(int)
        results['Applicants'] = results['Applicants'].fillna(0).astype(int)
        self.results = results

    def save_complete_reports(self):
        self.results.to_csv('job_activity_report.csv', index=False)

    def prepare_emails(self):
        self.names = self.results['Name'].unique()
        self.subject = email_handler.prepare_subject(self.start_date, self.end_date)

    def send_emails(self):
        for name in self.names:

            if self.single_recruiter is not None:
                name = self.single_recruiter

            recruiter_results = self.results[self.results['Name'] == name]
            first_name = email_handler.get_first_name(name)
            emails = recruiter_results['Email'].unique()
            assert(len(emails) == 1)
            email = emails[0]
            recruiter_results = report_handler.ready_recruiter_results(recruiter_results)
            html_frame = email_handler.to_html(recruiter_results)
            email_message = email_handler.create_message(first_name, html_frame)
            email = email_handler.prepare_reporting_email(to='joshuabragge@gmail.com', subject=subject, html=email_message)
            email_handler.send_email(email)

            if self.env == 'test':
                break
