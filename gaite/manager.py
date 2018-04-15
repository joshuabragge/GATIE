
from ruamel.yaml import YAML
import datetime
import time
import pandas as pd

import logger
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
# self.load function for reading from files


class ProcessManager(Env):
    def __init__(self, env):
        self.logger = logger.Logger(logger_name='Manager', filename='gaite.log')
        self.logger.logger.info('Logger Initialized..')

        self.shared_state = env.shared_state

        yaml = YAML()
        self.config = yaml.load(open('runtime.yaml', 'r'))

        self.logger.logger.info('runtime.yaml loaded')

        # load in runtime parameters
        self.env = self.shared_state['env']
        self.logger.logger.info('Environment Settings: {}'.format(self.env))

        if self.shared_state['recruiter'] is None:
            self.active_recruiters = self.config[self.env]['analytics']['recruiter']
        else:
            self.active_recruiters = []
            self.active_recruiters.append(self.shared_state['recruiter'])

        self.logger.logger.info('Active Recruiters: {}'.format(self.active_recruiters))

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
        self.from_address = self.sent_from_name + "<" + self.sent_from_address + ">"
        self.send_off_emails = self.config[self.env]['emails']['send_emails']

        self.delivery_time = self.config[self.env]['emails']['delivery_time']

        self.logger.logger.info("Sending emails: {}".format(self.send_off_emails))
        self.logger.logger.info("Save results: {}".format(self.save))
        self.logger.logger.info("Sending results to {}".format(self.to_address))
        self.logger.logger.info("Sending results from {}".format(self.sent_from_address))
        self.logger.logger.info("Sending results with name {}".format(self.sent_from_name))

        # initialize report params
        self.report_views = None
        self.report_applicants = None
        self.report = None
        self.web_page_details = None
        self.final_report = None
        self.start_date, self.end_date = report_manager.create_date_range(start_date, end_date)

        self.logger.logger.info('Analytics range from {} to {}'.format(self.start_date, self.end_date))

        # initialize email params
        self.names = None
        self.subject = None
        self.all_recipients = None

    def trigger(self):
        self.get_analytics_reports()

        if self.save:
            self.save_analytics_reports()

        report = self.ready_reports()
        web_page_details = self.get_report_details(report)

        if self.save:
            self.save_web_page_details()

        final_report = self.add_report_details(report, web_page_details)

        if self.save:
            self.save_complete_reports()

        # final_report = self.load_complete_report()

        self.prepare_emails()

        self.send_emails(final_report)

    def get_analytics_reports(self):
        """
        Initializes the connection to google analytics
        grabs two reports for job ad web pages - view and applicants
        :return: None
        """
        analytics = report_manager.initialize_analyticsreporting()
        self.logger.logger.info("Initializing Google Analytics")
        self.report_views = report_manager.get_weekly_views(analytics, self.start_date, self.end_date)
        self.logger.logger.info("Grabbed View Report")
        self.report_applicants = report_manager.get_weekly_applicants(analytics, self.start_date, self.end_date)
        self.logger.logger.info("Grabbed Applicants Report")
        return None

    def save_analytics_reports(self):
        """
        saves the applicants and views reports as CSVs in working dir
        :return: boolean
        """
        try:
            self.report_applicants.to_csv('weekly_applicants.csv', index=False)
            self.logger.logger.info("Saving Reporting Applicants as weekly_applicants.csv")
            self.report_views.to_csv('weekly_views.csv', index=False)
            self.logger.logger.info("Saving Reporting Views as weekly_views.csv")
        except:
            self.logger.logger.error('FAILED TO SAVE ANALYTICS REPORT RESULTS')
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
            self.logger.logger.info("Saved Web Page Scraping Results as web_pages.csv")
        except:
            self.logger.logger.error('FAILED TO SAVE WEB PAGE SCRAPING RESULTS')
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
        except:
            self.logger.logger.error('FAILED TO SAVE COMPLETE REPORT')
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
                    self.logger.logger.info('Sent email about {} to {}'.format(employee, to_address))

    def run(self):
        # time controller
        report_sent = False
        sleeper = 1800
        self.logger.logger.info("Run executed...")
        self.logger.logger.info("Sleeping for {} seconds".format(sleeper))
        delivery_day = self.delivery_time[0]
        delivery_hour = self.delivery_time[1]
        self.logger.logger.info("Delivery on {} at {}".format(delivery_day, delivery_hour))

        while True:
            try:
                now = datetime.datetime.now()

                if report_sent is False:

                    if now.weekday() == delivery_day:
                        self.logger.logger.debug("Weekday {}".format(delivery_day))
                        if now.hour == delivery_hour:
                            self.logger.logger.debug("Hour {}".format(delivery_hour))
                            report_sent = True
                            self.logger.logger.info("Executing Trigger")
                            self.trigger()
                    else:
                        pass

                if report_sent is True:
                    if now.weekday() == 1:
                        self.logger.logger.info("Resetting trigger...")
                        report_sent = False
                time.sleep(sleeper)
            except (KeyboardInterrupt, SystemExit):
                self.logger.logger.info("Exiting program due to KeyBoardInterrupt")
                raise



