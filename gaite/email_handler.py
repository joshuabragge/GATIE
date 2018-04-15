import requests
from secrets import config


def create_message(name, html_frame, env='test'):
    if env == 'test':
        message = '''<p>
        <span style="font-family: calibri, sans-serif; font-size: 11pt;">Hi $name,</span>
    </p><p>
        <span style="font-family: calibri, sans-serif; font-size: 11pt;">Welcome to your weekly job impression report: </span> 
        $html_frame 
    </p> <p>
        <span style="font-family: calibri, sans-serif; font-size: 11pt;">Happy Hunting! </span>
    </p><p>
        <span style="font-family: calibri, sans-serif; font-size 8pt;">Congratulations, you've been chosen for beta testing! This report details the approximate number of views and applicants for jobs posted on The Headhunters website. Please note, this does not include Indeed page views unless applicants click "View on Company Site". If a job is no longer on the headhunters website, the statistics will not be included. All information has been pulled from Google Analytics. Please send all comments, suggestions or errors to josh@bragge.ca</span>
            <br />
        </span>
    </p>'''
    else:
        message = '''<p>
                <span style="font-family: calibri, sans-serif; font-size: 11pt;">Hi $name,</span>
            </p>
            <p>
                <span style="font-family: calibri, sans-serif; font-size: 11pt;">Welcome to your weekly job impression report: </span> 
                $html_frame 
            </p>
            <p>
                <span style="font-family: calibri, sans-serif; font-size: 11pt;">Happy Hunting! </span>
            </p>
            <p>
                <span style="font-family: calibri, sans-serif; font-size 8pt;">This report details the approximate number of views and applicants for jobs posted on The Headhunters website. Please note, this does not include Indeed page views unless applicants click "View on Company Site". If a job is no longer on the headhunters website, the statistics will not be included. All information has been pulled from Google Analytics. Please send all comments, suggestions or errors to josh@bragge.ca</span>
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

    header = '<th></th>      <th></th>      <th>Views</th>      <th>JobId</th>      <th>Applicants</th>      <th>Applicants/Views</th>    </tr>    <tr>      <th>Job</th>      <th>Source</th>'
    header_replace = '<th>Job</th>      <th>Source</th>      <th>Views</th>      <th>JobId</th>      <th>Applicants</th>      <th>Applicants/Views</th>    </tr>    <tr>      <th></th>      <th></th>'
    html_frame = html_frame.replace(header, header_replace)

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
        background-color: #888888;
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
    subject = 'Impression Report {}'.format(date)
    return subject


def prepare_reporting_email(to_address, from_address, subject, html):
    to_address = to_address
    from_address = from_address
    data = {"from": from_address,
              "to": to_address,
              "subject": subject,
              "html": html}
    return data


def send_email(data):
    return requests.post(
                config.email_base_url,
                auth=("api", config.email_api_key),
                data=data)

