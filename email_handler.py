import pandas as pd
import requests

def create_message(name, html_frame):
    message = '''<p>
    <span style="font-family: calibri, sans-serif; font-size: 11pt;">Hi $name,</span>
</p>
<p>
    <span style="font-family: calibri, sans-serif; font-size: 11pt;">Welcome to your weekly job activity report: </span> 
    $html_frame 
</p>
<p>
    <span style="font-family: calibri, sans-serif; font-size: 11pt;">Happy Hunting! </span>
</p>
<p>
    <span style="font-family: calibri, sans-serif; font-size 8pt;">This report details the number of views and the approximate number of applicants for jobs posted on theheadhunters.ca. Please note, if a job is no longer on the headhunters website, the statistics will not be included. All information has been pulled from Google Analytics. Please send all comments, suggestions or errors to josh@bragge.ca</span>
        <br />
    </span>
</p>'''
    message = message.replace("\n", "<pre>")
    message = message.replace("\t", "")
    message = message.replace("$name", name)
    message = message.replace("$html_frame", html_frame)
    return message


def html_cleanup(html_frame):
    
    html_frame = html_frame.replace("\n", "")
    
    h2 = '<h2>&nbsp;</h2>'
    h2_replace = ''
    html_frame = html_frame.replace(h2, h2_replace)
    
    pre = '<pre>&nbsp;</pre>'
    pre_replace = ''
    html_frame = html_frame.replace(pre, pre_replace)
    
    pre_2 = '<pre>    </pre>'
    pre_2_replace = ''
    html_frame = html_frame.replace(pre_2, pre_2_replace)
    
    return html_frame


def get_first_name(name):
    name = name.split(' ')[0]
    return name
    

def to_html(df, title=''):
    '''
    Write an entire dataframe to an HTML file with nice formatting.
    '''

    result = '''<html>
<head>
<style>

    h2 {
        text-align: center;
        font-family: calibri, sans-serif;
    }
    table { 
        margin-left: auto;
        margin-right: auto;
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
    }
    th, td {
        padding: 5px;
        text-align: center;
        font-family: calibri, sans-serif;
        font-size: 90%;
    }
    table tbody tr:hover {
        background-color: #dddddd;
    }
    .wide {
        width: 90%; 
    }

</style>
</head>
<body>
    '''
    result += df.to_html(classes='wide', escape=False)
    result += '''
</body>
</html>
'''
    result = html_cleanup(result)
    return result

def prepare_subject(start_date,end_date):
    date = '{} - {}'.format(start_date.strftime('%b. %d'), end_date.strftime('%b. %d'))
    subject = 'Job Activity Report {}'.format(date)
    return subject

def prepare_reporting_email(to, subject, html):
    to = ["joshuabragge@gmail.com"]
    from_ = "Job Activity Reporting <reporting@bragge.ca>"
    data={"from": from_,
          "to": to,
          "subject": subject,
          "html": html}
    return data

def send_email(data):
    return requests.post(
                email_base_url,
                auth=("api", email_api_key),
                data=data)

