import pandas as pd
import re
import numpy as np
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

#Obtain target urls
targetlist = pd.read_csv('/Users/apple/Desktop/connect/waiting.csv')
targetlist = list(targetlist['url'])
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
info_base = MongoInfo('land') #Define name of the info database
varlist = ['宗地标识', '宗地编号', '宗地座落', '所在行政区', '原土地使用权人', '现土地使用权人',\
			'土地面积\(公顷\)', '土地用途', '土地使用权类型', '土地使用年限', '土地利用状况',\
			'土地级别', '转让方式', '转让价格\(万元\)', '成交时间', 'Gisquest']
datadict = {}
datadict.setdefault('_id')#set id to be traced with __get__

for i in varlist[0:-1]:
	datadict.setdefault(i)

for count, url in enumerate(targetlist):
	print ('%.2f%%: %s'%(count / len(targetlist) * 100, count))
	# try:
	# 	code = webpage_cache[url]['code']
	# 	print (code)
	# except:
	# 	code = None
	# if code != 200:
	# 	faillist.append(count)
	# 	continue
	infodict = datadict.copy()
	try:
		html = D(url)
		if html == '':
			raise
	except Exception as e:
		print ('fail getting html, error: %s'%e)
		continue
	infodict['_id'] = url
	html = html.decode('gbk', errors = 'ignore')
	regex = '(?<=%s)(?:<[\s\S]*?>)*([\s\S]*?)(?:<[\s\S]*?>)*(?=%s)'
	for idx in range(len(varlist) - 1):
		try:
			info = usere(regex%('%s：','%s')%(varlist[idx], varlist[idx + 1]), html)[0]
		except Exception as e:
			print ('regex error: %s'%e)
			continue
		infodict[varlist[idx]] = info
	info_base.push(infodict)
	# webpage_cache.db.webpage.remove({'_id':url})	#清除已获取信息缓存
