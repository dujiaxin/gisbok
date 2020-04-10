# -*- coding: utf-8 -*-
import re
import os
import ssl
import time
import urllib
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from queue import Empty
from multiprocessing import Process, Queue
from tqdm import tqdm
import requests
import random
import json
import string


def replace_marks(string, maxsplit=0):
    # replace special marks
    # string.replace('\\n','').replace('\\t','')
    markers = "*", "/", "+", ",", '"'
    regexPattern = '|'.join(map(re.escape, markers))
    return re.sub(regexPattern, ' ', string)


def mkdir(path):
    folder = os.path.exists(path)
    if not folder:
        os.makedirs(path)
        print("---  created Scholar folder...  ---")
    else:
        print("---  Scholar folder has already exist!  ---")


def Download(q):
    while True:
        file_name = None
        url = None
        try:
            info = q.get(block=False)
            file_name = info["file_name"]
            url = info["url"]
            ssl._create_default_https_context = ssl._create_unverified_context
            d = urllib.request.URLopener()
            d.retrieve(url, "D://Scholar/" + file_name + ".pdf")
            print(file_name, url)
        except Empty:
            break


class URLPARSE():
    def __init__(self):
        self.search_scholar = "gis "
        # self.chrome_driver_path = "D:\chromedriver\chromedriver.exe"
        self.google_scholar = "https://scholar.google.com/scholar?hl=en"
        self.path = "./data/"#os.path.join('D:', 'Scholar')
        self.driver = self.Setdriver()
        self.counter = 0
        self.needDownloadUrls = []
        self.needSaveHTMLs = []

    def start(self, keywords):
        try:
            self.OpenSearchIndex()
            self.SearchScolar(keywords)
            self.Parse()
        finally:
            self.End()

    def Setdriver(self):
        option = webdriver.ChromeOptions()
        prefs = {'profile.default_content_settings.popups': 0, 'download.default_directory': self.path}
        option.add_experimental_option('prefs', prefs)
        option.add_argument('headless')
        driver = webdriver.Chrome()
        # driver = webdriver.Chrome('D:\chromedriver\chromedriver.exe')
        # driver = webdriver.Chrome(self.chrome_driver_path, chrome_options=option)

        return driver

    def OpenSearchIndex(self):
        self.driver.get(self.google_scholar)
        session_id = self.driver.session_id

        print('session_id:', session_id)

    def SearchScolar(self, searchTarget):
        target = searchTarget
        q = self.driver.find_element_by_name('q')
        q.send_keys(target)

        from selenium.webdriver.common.keys import Keys
        q.send_keys(Keys.RETURN)

    def NextPage(self):
        time.sleep(random.randint(2,15))
        if self.counter >= 1000:
            return False
        try:
            nextButton = WebDriverWait(self.driver, 600).until(lambda d: d.find_element_by_link_text("Next"))
            nextButton.click()
            self.counter += 1
            return True
        except Exception as e:
            return False

    def Parse(self):
        while (True):
            mid = WebDriverWait(self.driver, 600).until(lambda d: d.find_element_by_id("gs_res_ccl_mid"))
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            lis = []
            dic = {}

            for item in soup.find_all('div', class_="gs_r gs_or gs_scl"):
                if item.h3.text.startswith("[CITATION]"):
                    continue
                # title = item.select('gs_rt a').text
                ref = item.h3.text
                try:
                    ref_href = item.find('div', class_="gs_or_ggsm").find('a').get("href")
                except AttributeError:
                    continue
                dic = {'ref': ref, 'ref_href': ref_href}
                lis.append(dic)
            for c in lis:
                if (c['ref_href'].endswith('.pdf')):
                    file_name = c['ref'].strip("[PDF]").strip(" ")
                    _info = {}
                    _info["file_name"] = file_name
                    _info["url"] = c['ref_href']
                    self.needDownloadUrls.append(_info)
                else:
                    file_name = c['ref'].strip("[HTML]").strip(" ")
                    _info = {}
                    _info["file_name"] = file_name
                    _info["url"] = c['ref_href']
                    self.needSaveHTMLs.append(_info)

            if not self.NextPage():
                break

    def End(self):
        # self.driver.close()
        self.driver.quit()

    def getDownloadList(self):
        return self.needDownloadUrls

    def getHTMLList(self):
        return self.needSaveHTMLs


def saveURLs(keywords):

    pOBJ = URLPARSE()
    pOBJ.start(keywords)
    downloadList = pOBJ.getDownloadList()
    saveList = pOBJ.getHTMLList()
    with open(keywords+'_pdfs.json', 'w', encoding='utf-8') as f:
        json.dump(downloadList, f, ensure_ascii=False, indent=4)
    with open(keywords+'_htmls.json', 'w', encoding='utf-8') as f:
        json.dump(saveList, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    keywords = "Analytics Modeling GIS"
    # saveURLs(keywords)
    with open(keywords+'_pdfs.json', 'r', encoding='utf-8') as f:
        downloadList = json.load(f)
    with open(keywords+'_htmls.json', 'r', encoding='utf-8') as f:
        saveList = json.load(f)

    translator = str.maketrans(string.punctuation, ' ' * len(string.punctuation))
    path = os.path.join('./data/', keywords)
    mkdir(path)

    #download pdf
    for pdfLink in tqdm(downloadList):
        try:
            time.sleep(random.randint(2,12))
            to_file_path = path +'/'+ pdfLink["file_name"].translate(translator) + ".pdf"

            if(pdfLink["url"].startswith("ftp")):
                print(pdfLink["url"])
                continue

            r = requests.get(pdfLink["url"])
            with open(to_file_path, "wb") as f:
                f.write(r.content)
        except:
            print(pdfLink)

    for webpage in tqdm(saveList[20:]):
        try:
            time.sleep(random.randint(2,12))
            r = requests.get(webpage["url"])
            to_file_path = path +'/'+ webpage["file_name"].translate(translator) + ".html"
            with open(to_file_path, "w") as f:
                f.write(r.text)
        except:
            print(webpage)

    # downloadQueue = Queue()
    #
    # for d in downloadList:
    #     print(d)
    #     downloadQueue.put(d, block=False)
    #
    # Processes = [Process(target=Download, args=(downloadQueue,)) for i in range(4)]
    #
    # for p in Processes:
    #     p.start()
    # for p in Processes:
    #     p.join()

    print("Scholar download Finished!")