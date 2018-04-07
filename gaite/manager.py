
from ruamel.yaml import YAML

from gaite import email_handler, web_handler, report_manager, report_handler, environment


class ProcessManager(Env):
    def __init__(self):
        yaml = YAML()
        self.config = yaml.load('../runtime.yaml')
        self.env = self._shared_params['env']

        if self._shared_params['recruiter'] is None:
            self.active_recruiters = self.config[self.env]['recruiter']
        else:
            self.active_recruiters = list(self._shared_params['recruiter'])

        self.save = self.config[self.env]['results']['save']
        start_date = None
        end_date = None
        self.weekly_applicants = None
        self.weekly_views = None
        self.web_pages = None
        self.results = None
        self.names = None
        self.subject = None

        self.start_date, self.end_date = self.reporting_manager.create_date_range(start_date, end_date)


    def run(self):

        self.get_analytics_reports()

        if self.save:
            self.save_analytics_reports()

        report = self.ready_reports()
        web_page_details = self.get_report_details(report)

        if self.save:
            self.save_web_details()

        self.parse_web_pages()

        if self.save:
            self.save_web_report()

        self.add_report_details(report, web_page_details)

        if self.save:
            self.self.save_complete_reports()

        self.prepare_emails()

        self.send_emails()

    def get_analytics_reports(self):
        """
        initalizes the connection to google analytics
        grabs two reports for job ad webpages - view and applicants
        """
        analytics = report_manager.initialize_analyticsreporting()
        self.report_views = report_manager.get_weekly_views(analytics, self.start_date, self.end_date)
        self.report_applicants = report_manager.get_weekly_applicants(analytics, self.start_date, self.end_date)

    def save_analytics(self):
        """
        saves the applicants and views reports as csvs in working dir
        """
        self.report_applicants.to_csv('weekly_applicants.csv',index=False)
        self.report_views.to_csv('weekly_views.csv',index=False)

    def ready_reports(self):
        """
        transforms google analytics reports into a usable format
        merges all reports together
        """
        report_applicants = report_handler.ready_frame(self.report_applicants, types='applicants')
        report_views = report_handler.ready_frame(self.report_views, types='views')
        self.report = report_handler.merge(report_views, report_applicants)
        assert(len(report.shape) > 0)
        return self.report

    def get_report_details(self, report):
        """
        transform report for web scraping
        scrap recruiter name and job name from each webpage
        create recruiter email address
        """
        web_pages = report_handler.create_web_pages(report)
        web_pages['Details'] = web_pages['Page'].apply(lambda x: web_handler.get_stuff_from_url(x))
        web_pages[['Name', 'Email', 'Job']] = web_pages['Details'].str.split('|', expand=True)
        web_pages = web_pages.drop('Details', axis=1)
        web_pages['Page'] = web_pages['Page'].apply(lambda x: x.replace('http://www.theheadhunters.ca', ''))
        self.web_page_details = web_pages
        return self.web_page_details

    def save_web_page_details(self):
        self.web_page_details.to_csv('web_pages.csv', index=False)

    def add_report_details(self, report, web_page_details):
        results = report.merge(web_page_details, on='Page')
        results = results.dropna(subset=['Email'])
        results['Page'] = 'http://www.theheadhunters.ca' + results['Page']
        results['Views'] = results['Views'].astype(int)
        results['Applicants'] = results['Applicants'].fillna(0).astype(int)
        self.final_report = results

    def save_complete_reports(self):
        self.final_report.to_csv('job_activity_report.csv', index=False)

    def prepare_emails(self):
        self.all_recepiants = self.final_report['Name'].unique()
        self.subject = email_handler.prepare_subject(self.start_date, self.end_date)

    def send_emails(self):
        for name in self.all_recepiants:
            if self.active_recruiters is None:
                recruiter = name
            else:
                for active_recruiter in active_recruiters:
                    if active_recruiter == name:
                        employee = name
                    else:
                        employee = None
            if employee is not None:
                recruiter_results = self.final_report[self.final_report['Name'] == employee]
                first_name = email_handler.get_first_name(employee)
                emails = employee_results['Email'].unique()
                assert(len(emails) == 1)
                email = emails[0]
                employee_results = report_handler.ready_recruiter_results(employee_results)
                html_frame = email_handler.to_html(recruiter_results)
                email_message = email_handler.create_message(first_name, html_frame)
                email = email_handler.prepare_reporting_email(to='joshuabragge@gmail.com', subject=self.subject, html=email_message)
                email_handler.send_email(email)

                if self.env == 'test':
                    break
