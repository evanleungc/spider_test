import pandas as pd
import re
from process_crawler import process_crawler
from mongo_queue import MongoQueue
from mongo_cache import MongoCache
from mongo_info import MongoInfo
from downloader import Downloader

crawl_queue = MongoQueue()
webpage_cache = MongoCache()
DEFAULT_AGENT = {}
DEFAULT_DELAY = 5
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 100
DEFAULT_PROXY_LIST = '/Users/apple/Desktop/connect/proxylist/proxies.csv'
DEFAULT_COOKIE = {}

D = Downloader(delay=DEFAULT_DELAY, user_agent=DEFAULT_AGENT, proxies=DEFAULT_PROXY_LIST, \
        cookies = DEFAULT_COOKIE, num_retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT, \
        opener=None, cache=MongoCache())

def usere(regex, getcontent): #定义使用正则表达式的函数
    pattern = re.compile(regex)
    content = re.findall(pattern, getcontent)
    return content

#Clear Cache
# crawl_queue.clear()
# webpage_cache.clear()


#Obtain target urls
startdate = '20180414'
enddate = '20180415'
city = '0100'
totalpage = 20
targetlist = []
for pageno in range(1, totalpage + 1):
	url = 'http://www.bthhotels.com/listasync/beijing?cityCode=%s&SelectArea=&SignleAreaFilter=&priceRage=&feature=&cityName=beijing&beginDate=%s%%2F%s%%2F%s&endDate=%s%%2F%s%%2F%s&orderBy=&pageNo=%s&device=&key=&keyDescript=&Brands=&ForH5Page=true&ActivityCode='%(city, startdate[0:4], startdate[4:6], startdate[6:8], enddate[0:4], enddate[4:6], enddate[6:8], pageno)
	targetlist.append(url)
#Store urls into cache
for i in targetlist:
	crawl_queue.push(i)

#multiprocessing with crawler
process_crawler(targetlist[0], max_threads = 30)
