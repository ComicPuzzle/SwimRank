from hyperlink import URL
import pandas as pd
from datetime import datetime
import requests
import pypdf
from bs4 import BeautifulSoup
import pdfplumber
from collections import defaultdict
import json

def str_to_datetime(val):
    if ":" in val:
        date_format = "%M:%S.%f"
    else:
        date_format = "%S.%f"
    return datetime.strptime(val, date_format)

def get_links():
    URL = "https://www.usaswimming.org/times/time-standards"
    page = requests.get(URL)

    soup = BeautifulSoup(page.content, "lxml")
    most_recent_class = soup.find_all(attrs={'data-usas-category': 'Content_TopResources_Index_Div-1-'})
    links = []
    meets = []
    for link in most_recent_class:
        meets.append(link.find('div').get_text())
        links.append('https:' + link.get('href'))
    return meets, links

def parse_link(link, meet):
    events = ['50 FR', '100 FR', '200 FR', '400/500 FR', '800/1000 FR', '1500/1650 FR',
              '50 BK', '100 BK', '200 BK',
              '50 BR', '100 BR', '200 BR',
              '50 FL', '100 FL', '200 FL',
              '100 IM', '200 IM', '400 IM']
    response = requests.get(link)
    with open("temp.pdf", "wb") as f:
        f.write(response.content)
    rows1 = []
    rows2 = []
    with pdfplumber.open("temp.pdf") as pdf:
        for i in range(len(pdf.pages)):
            page = pdf.pages[i]
            words = page.extract_words(use_text_flow=True)
            lines = defaultdict(list)
            for w in words:
                y = round(w["top"], 1)
                lines[y].append(w["text"])
            for y in sorted(lines):
                line = lines[y]
                if i == 0:
                    rows1.append(line)
                else:
                    rows2.append(line)

    try:
        value = " ".join(rows1[0])
    except:
        return {}
    i = 0
    while value != "50 FR":
        i += 1
        value = " ".join(rows1[i])
    rows1 = rows1[i-3:]
    times = {}
    standards = {}

    for i in range(2, len(rows1)-1, 2):
        if " ".join(rows1[i+1]) in events:
            times[" ".join(rows1[i+1])] = rows1[i]
    standards[" ".join(rows1[0])] = times

    times = {}
    if len(rows2) > 0:
        value = " ".join(rows2[0])
        i = 0
        while value != "50 FR":
            i += 1
            value = " ".join(rows2[i])
        rows2 = rows2[i-3:]

        for i in range(2, len(rows2)-1, 2):
            if " ".join(rows2[i+1]) in events:
                times[" ".join(rows2[i+1])] = rows2[i]
        standards[" ".join(rows2[0])] = times

    return standards

if __name__ == "__main__":
    meets, links = get_links()
    standards = {}
    for i in range(len(links)):
        meet_standards = parse_link(links[i], meets[i])
        if meet_standards != {}:
            standards[meets[i]] = meet_standards
    file_path = 'upcoming_championship_standards.txt'
    with open(file_path, 'w') as file:
        json.dump(standards, file, indent=4)