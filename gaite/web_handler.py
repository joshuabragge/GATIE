import requests
from bs4 import BeautifulSoup
import time
import numpy as np


def get_stuff_from_url(web_url):
    try:
        print(web_url)
        soup = get_soup_from_url(web_url)
        name = get_name_from_soup(soup)
        email = create_email(name)
        title = get_title_from_soup(soup)
        row = name + '|' + email + '|' + title
        time.sleep(0.5)
        return row
    except:
        return np.nan
    

def get_title_from_soup(soup):
    title = soup.find('h2', attrs={'class': 'entry-title'})
    return title.text


def get_name_from_soup(soup):
    page_intro = find_intro_in_soup(soup)
    name = remove_intro_leave_name(page_intro)
    return name


def get_soup_from_url(web_url):
    url = requests.get(web_url)
    data = url.text
    soup = BeautifulSoup(data, 'html5lib')
    return soup


def find_intro_in_soup(soup):
    intro = soup.find('div', attrs={'class': 'hello'})
    return intro.text


def remove_intro_leave_name(text):
    name = text.replace("Hi, I'm ", "")
    return name


def create_email(name):
    f1 = name.split(' ')[0][0].lower()
    f2 = name.split(' ')[1].lower()
    email = f1 +  f2 + '@theheadhunters.ca'
    return email
