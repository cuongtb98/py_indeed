import csv
import re
from webbrowser import Chrome
import requests
import cloudscraper
from bs4 import BeautifulSoup
import threading
import html_text
import time
import random
from requests_html import HTMLSession
session = HTMLSession()

tab = None
if tab == "chrome1":
    category_list = ["Product Marketing"]
elif tab == "chrome2":
    category_list = ["Content Marketing"]
elif tab == "chrome3":
    category_list = ["Tech"]
elif tab == "chrome4":
    category_list = ["Design"]

with open('data_set.csv', 'a', newline='',  encoding='utf-8') as csvfile:
    fieldnames = ['Title', 'Category', 'Content']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    def get_location():
        url = "https://www23.statcan.gc.ca/imdb/p3VD.pl?Function=getVD&TVD=53971"
        source = session.get(url)
        soup = BeautifulSoup(source.text, 'html.parser')
        items = soup.find('tbody').find_all('tr')
        location = []
        for itm in items:
            location.append(itm.find('td').text.strip())
        return location


    def get_pages(q,l):
        pages_number = 0
        for i in range(10):
            link = f'https://www.indeed.com/jobs?q={q}&l={l}&start=666'
            session = requests.session()
            scraper = cloudscraper.create_scraper(sess=session)
            html = scraper.get(link).content
            soup = BeautifulSoup(html, 'html.parser')
            page = soup.find('nav', class_='css-jbuxu0 ecydgvn0')
            
            if page == None:
                pass
            else:
                pages_number = page.find('button', class_='css-1cpyzlr e8ju0x51').text.strip()
                return int(pages_number)+1
        return pages_number

    def filter_content(content):
        content = re.sub('\s+', ' ', content)
        return content.strip()

    def process_location(dict, c, location):
        print(f"=============== BEGIN CATEGORY {c} ===============")
        temp = 0
        dict_val = {}
        list_temp = []
        for l in location:
            print(f"=====> Category: {c} - Location: {l}")
            page = get_pages(c,l)
            temp = temp + page
            if temp < 200:
                list_temp.append(l)
            else:
                break
        print(f"========= END CATEGORY {c} - job: {temp*15} =========")
        dict_val["location"] = list_temp
        dict_val["total"] = temp
        dict[c] = dict_val

    def process_input():
        location = get_location()
        dict = {}
        threads = []
        for c in category_list: 
            threads += [threading.Thread(target=process_location, args=(dict, c, location, ))]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return dict

    def get_data(jk,category):
        link = f'https://www.indeed.com/viewjob?jk={jk}'
        print('__craw: ', link)
        scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})
        html = scraper.get(link).content
        soup = BeautifulSoup(html, 'html.parser')
        try:
            title = soup.find('div', class_='jobsearch-JobInfoHeader-title-container').text.strip() 
            content = soup.find('div', id='jobDescriptionText')
            content = html_text.extract_text(str(content), guess_layout=False)
            content = filter_content(content)

            writer.writerow({'Title': title, 'Category': category, 'Content': content})
        except:
            pass


    Category = process_input()
    for ca in Category:
        print(f"----------- {ca} -----------")
        location = Category[ca]["location"]
        total = Category[ca]["total"]
        for loc in location:
            for i in range(0, total):
                print("===========> Crawl page: ", i)
                url_page = f'https://www.indeed.com/jobs?q={ca}&l={loc}&start={i*10}'
                print(f" ======================== {url_page} ======================== ")
                scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})
                html = scraper.get(url_page).content
                soup = BeautifulSoup(html, 'html.parser')
                data = soup.find_all('h2', class_='jobTitle')
                threads = []
                for i in data:
                    print(i.find('a')['data-jk'])
                    jk = i.find('a')['data-jk']
                    threads += [threading.Thread(target=get_data, args=(jk,ca ))]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()
