
from ruamel.yaml import YAML
import datetime
import time
import pandas as pd

from gaite import email_handler
from gaite import web_handler
from gaite import report_handler
from gaite import report_manager

from gaite.environment import Env

# todo
# confirm email addresses when the recepiant is blank
# send master copy to management
# add failsafe function
# logger

class ProcessManager(Env):
    def __init__(self, env):
        self.shared_state = env.shared_state

        yaml = YAML()
        self.config = yaml.load(open('runtime.yaml', 'r'))

        # load in runtime parameters
        self.env = self.shared_state['env']

        if self.shared_state['recruiter'] is None:
            self.active_recruiters = self.config[self.env]['analytics']['recruiter']
        else:
            self.active_recruiters = []
            self.active_recruiters.append(self.shared_state['recruiter'])
        print(self.active_recruiters)
        if self.shared_state['start_date'] is None:
            start_date = self.config[self.env]['analytics']['start_date']
        else:
            start_date = self.shared_state['start_date']

        if self.shared_state['end_date'] is None:
            end_date = self.config[self.env]['analytics']['end_date']
        else:
            end_date = self.shared_state['end_date']

        # load config params
        self.save = self.config[self.env]['results']['save']
        self.to_address = self.config[self.env]['emails']['recipients']

        self.sent_from_address = self.config[self.env]['emails']['sent_from_address']
        self.sent_from_name = self.config[self.env]['emails']['sent_from_name']
        print(self.sent_from_address, self.sent_from_name)
        self.from_address = self.sent_from_name + "<" + self.sent_from_address + ">"

        self.send_off_emails = self.config[self.env]['emails']['send_emails']

        # initialize report params
        self.report_views = None
        self.report_applicants = None
        self.report = None
        self.web_page_details = None
        self.final_report = None
        self.start_date, self.end_date = report_manager.create_date_range(start_date, end_date)

        # initialize email params
        self.names = None
        self.subject = None
        self.all_recipients = None

    def trigger(self):
        #self.get_analytics_reports()

        #if self.save:
        #    self.save_analytics_reports()

        #report = self.ready_reports()
        #web_page_details = self.get_report_details(report)

        #if self.save:
        #    self.save_web_page_details()

        #final_report = self.add_report_details(report, web_page_details)

        #if self.save:
        #    self.save_complete_reports()
        final_report = self.load_complete_report()
        self.prepare_emails()

        self.send_emails(final_report)

    def get_analytics_reports(self):
        """
        Initializes the connection to google analytics
        grabs two reports for job ad web pages - view and applicants
        :return: None
        """
        analytics = report_manager.initialize_analyticsreporting()
        self.report_views = report_manager.get_weekly_views(analytics, self.start_date, self.end_date)
        self.report_applicants = report_manager.get_weekly_applicants(analytics, self.start_date, self.end_date)
        return None

    def save_analytics_reports(self):
        """
        saves the applicants and views reports as CSVs in working dir
        :return: boolean
        """
        try:
            self.report_applicants.to_csv('weekly_applicants.csv', index=False)
            self.report_views.to_csv('weekly_views.csv', index=False)
        except:
            return False
        return True

    def ready_reports(self):
        """
        transforms google analytics reports into a usable format
        merges all reports together
        :return: pandas dataframe
        """
        report_applicants = report_handler.ready_frame(self.report_applicants, types='applicants')
        report_views = report_handler.ready_frame(self.report_views, types='views')
        self.report = report_handler.merge(report_views, report_applicants)
        assert(len(self.report.shape) > 0)
        return self.report

    def get_report_details(self, report):
        """
        transform report for web scraping
        scrap recruiter name and job name from each webpage
        create recruiter email address
        :returns: pandas dataframe
        """
        web_pages = report_handler.create_web_pages(report)
        web_pages['Details'] = web_pages['Page'].apply(lambda x: web_handler.get_stuff_from_url(x))
        web_pages[['Name', 'Email', 'Job']] = web_pages['Details'].str.split('|', expand=True)
        web_pages = web_pages.drop('Details', axis=1)
        web_pages['Page'] = web_pages['Page'].apply(lambda x: x.replace('http://www.theheadhunters.ca', ''))
        self.web_page_details = web_pages
        return self.web_page_details

    def save_web_page_details(self):
        try:
            self.web_page_details.to_csv('web_pages.csv', index=False)
        except:
            return False
        return True

    def add_report_details(self, report, web_page_details):
        """
        combine the web page scrapped data with the analytics data
        :param report pandas dataframe:
        :param web_page_details pandas dataframe:
        :return: final_results pandas dataframe
        """
        results = report.merge(web_page_details, on='Page')
        results = results.dropna(subset=['Email'])
        results['Page'] = 'http://www.theheadhunters.ca' + results['Page']
        results['Views'] = results['Views'].astype(int)
        results['Applicants'] = results['Applicants'].fillna(0).astype(int)
        self.final_report = results
        return self.final_report

    def save_complete_reports(self):
        try:
            self.final_report.to_csv('job_activity_report.csv', index=False)
            print('final report loaded')
        except:
            return False
        return True

    def load_complete_report(self):
        try:
            self.final_report = pd.read_csv('job_activity_report.csv')
        except:
            return False
        return self.final_report

    def prepare_emails(self):
        """
        load all potential recipients and craft the email subject
        :return:
        """
        self.all_recipients = self.final_report['Name'].unique()
        self.subject = email_handler.prepare_subject(self.start_date, self.end_date)

    def send_emails(self, final_report):
        """
        send off the analytics report emails
        :return:
        """

        for name in self.all_recipients:
            employee = None

            if len(self.active_recruiters) == 0:
                employee = name
            else:
                for active_recruiter in self.active_recruiters:

                    if active_recruiter == name:
                        employee = name
                        break
                    else:
                        employee = None

            if employee is not None:

                employee_results = final_report[final_report['Name'] == employee]
                first_name = email_handler.get_first_name(employee)
                emails = employee_results['Email'].unique()
                assert(len(emails) == 1)
                email_address = emails[0]

                employee_report = report_handler.ready_recruiter_results(employee_results)
                html_frame = email_handler.to_html(employee_report)
                email_message = email_handler.create_message(first_name, html_frame, env=self.env)

                if len(self.to_address) == 0:
                    to_address = [email_address]
                else:
                    to_address = self.to_address

                email = email_handler.prepare_reporting_email(to_address=to_address,
                                                              from_address=self.from_address,
                                                              subject=self.subject,
                                                              html=email_message)
                if self.send_off_emails:
                    email_handler.send_email(email)
                    print('sent email')
                print('email delivered to', to_address, employee)

    def run(self):
        # time controller
        report_sent = False
        while True:
            now = datetime.datetime.now()

            if report_sent is False:
                print('report_sent False')
                if now.weekday() == 6:
                    print('weekday 0')
                    if now.hour == 11:
                        print('hour 6')
                        report_sent = True
                        self.trigger()
                else:
                    pass

            if report_sent is True:
                print('report_sent True')
                if now.weekday() == 1:
                    print('next day')
                    report_sent = False
            print('sleeping')
            time.sleep(1800)


