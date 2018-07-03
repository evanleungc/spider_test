import pandas as pd
import re
import numpy as np
from process_crawler import process_crawler
from mongo_queue import MongoQueue
from mongo_cache import MongoCache
from mongo_info import MongoInfo
from downloader import Downloader
from lxml import etree

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

#Obtain target urls
startdate = '20180414'
enddate = '20180415'
city = '0100'
totalpage = 20
targetlist = []
for pageno in range(1, totalpage + 1):
	url = 'http://www.bthhotels.com/listasync/beijing?cityCode=%s&SelectArea=&SignleAreaFilter=&priceRage=&feature=&cityName=beijing&beginDate=%s%%2F%s%%2F%s&endDate=%s%%2F%s%%2F%s&orderBy=&pageNo=%s&device=&key=&keyDescript=&Brands=&ForH5Page=true&ActivityCode='%(city, startdate[0:4], startdate[4:6], startdate[6:8], enddate[0:4], enddate[4:6], enddate[6:8], pageno)
	targetlist.append(url)
priorlist = []
for url in targetlist:
	try:
		code = webpage_cache[url]['code']
		if code == 200:
			priorlist.append(url)
	except:
		continue
waitinglist = list(set(targetlist).difference(set(priorlist)))
targetlist = priorlist
targetlist.extend(waitinglist)

#start extracting info
info_base = MongoInfo('homeinn') #Define name of the info database
varlist = ['name', 'score', 'commentnum', 'location', 'price']
datadict = {}
datadict.setdefault('_id')#set id to be traced with __get__

for i in varlist[0:-1]:
	datadict.setdefault(i)

namelist = []
scorelist = []
commentnum = []

priceregex = '<strong>(.+?)</strong>'
pricelist = usere(priceregex, html)
count = len(priceregex)
for i in range(1, count):
	try:
		namelist.append('/html/body/ul[%s]/li[1]/div[2]/a/text()'%i)
		scorelist.append('/html/body/ul[%s]/li[1]/div[2]/p[1]/span[1]'%i)
		commentnum.append('/html/body/ul[%s]/li[1]/div[2]/p[1]/span[3]'%i)
	except Exception as e:
		print (e)
		break
for i in pricelist:
	usere("<var class='(.+?)'></var>", i)

for count, url in enumerate(targetlist):
	print ('%.2f%%: %s'%(count / len(targetlist) * 100, count))
	infodict = datadict.copy()
	try:
		html = D(url)
		if html == '':
			raise
	except Exception as e:
		print ('fail getting html, error: %s'%e)
		continue
	infodict['_id'] = url
	html = html.decode('utf8', errors = 'ignore')
	selector = etree.HTML(html)
	namelist = []
	scorelist = []
	commentnum = []
	price = []
	priceregex = '<strong>(.+?)</strong>'
	pricelist = usere(priceregex, html)
	count = len(pricelist)
	for i in range(1, count):
		namelist.append(selector.xpath('/html/body/ul[%s]/li[1]/div[2]/a/text()'%i))
		scorelist.append(selector.xpath('/html/body/ul[%s]/li[1]/div[2]/p[1]/span[1]/text()'%i))
		commentnum.append(selector.xpath('/html/body/ul[%s]/li[1]/div[2]/p[1]/span[3]/text()'%i))

	for i in pricelist:
		price.append(usere("<var class='(.+?)'></var>", i))
		datadict
		infodict[varlist[idx]] = info
	for var in varlist:
		datadict.setdefault(var, []).extend()
	info_base.push(infodict)
	# webpage_cache.db.webpage.remove({'_id':url})	#清除已获取信息缓存
