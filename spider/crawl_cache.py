import pandas as pd
import re
from process_crawler import process_crawler
from mongo_queue import MongoQueue
from mongo_cache import MongoCache
from mongo_info import MongoInfo
from downloader import Downloader

crawl_queue = MongoQueue()
crawl_queue.turn_down()
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
targetlist = pd.read_csv('/Users/apple/Desktop/connect/waiting.csv')
targetlist = list(targetlist['url'])

#Store urls into cache
for i in targetlist:
	crawl_queue.push(i)

#multiprocessing with crawler
process_crawler(targetlist[0], max_threads = 60)