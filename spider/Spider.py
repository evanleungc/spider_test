import requests
import time
import random
import re
import threading
import pandas as pd
from selenium import webdriver
from multiprocessing import Pool
from fake_useragent import UserAgent
from mongo_queue import MongoQueue

def usere(regex, getcontent): #定义使用正则表达式的函数
    pattern = re.compile(regex)
    content = re.findall(pattern, getcontent)
    return content

class Crawl(object):
	'''
	Attributes
	----------
	url: list
		a list of url to crawl
	cookie: str
		coolie to fake login
	ua: boolean value
		if True, use UserAgent
	proxy: boolean value
		if True, use proxy
	processes: int
		if process is 0, use no processes
		else, this attribute defines numbers of processes
	'''
	def __init__(self, cookie = None, ua = True, proxy = True, processes = 1):
		self.cookie = cookie
		self.is_ua = ua
		if self.is_ua == True:
			self.ua = UserAgent()
		self.is_proxy = proxy
		self.processes = processes 
		self.sele = False
		self.content = [[] for i in range(13)]
		self.connect_path = '/Users/apple/Desktop/connect/'
		
	def get_html(self, url, retry = 5):
		'''
        	Return url's html
        	
        	Parameters
        	----------
        		
		'''
		if 'http://' not in url:
			url = 'http://' + url
		if self.is_ua == True:
			head = {'User-Agent': self.ua.random}
		else:
			head = {}
		if self.is_proxy == True:
			ip, port = self.get_proxy()
			proxy = {'http':'http://%s:%s'%(ip, port)}
		else:
			proxy = {}
		if self.cookie != None:
			cookie = {'cookies': self.cookie}
		else:
			cookie = {}
		for i in range(retry):
			try:
				html = requests.get(url, timeout = 10, proxies = proxy, headers = head, cookies = cookie).content
				print (url)
				return html
			except Exception as e:
				print ('retry getting html, error:%s'%e)
		return 'Error'

	def switch_page(self, url):
		if self.sele == False:
			browser = webdriver.Chrome()
			self.sele = True
		returnlist = []
		browser.get(url)
		html = browser.page_source
		retlist = usere('<a href="(default.aspx?.+?)" target', html)
		retlist = ['www.landchina.com/' + i for i in retlist]
		retlist = [i.replace('&amp;', '&') for i in retlist]
		returnlist.extend(retlist)
		for i in range(2):
			browser.find_element_by_xpath('//*[@id="mainModuleContainer_1438_1439_1801_tdExtendProContainer"]/table/tbody/tr[1]/td/table/tbody/tr[2]/td/div/table/tbody/tr/td[2]/a[12]').click()
			html = browser.page_source
			retlist = usere('<a href="(default.aspx?.+?)" target', html)
			retlist = ['http://www.landchina.com/' + i for i in retlist]
			retlist = [i.replace('&amp;', '&') for i in retlist]
			returnlist.extend(retlist)
			try:
				gotlist = pd.read_csv(self.connect_path + 'got.csv')
				gotlist = list(got['url'])
			except:
				gotlist = []
			returnlist = list(set(returnlist).difference(set(gotlist)))
			returndf = pd.DataFrame({'url':returnlist})
			returndf.to_csv(self.connect_path + 'waiting.csv', index = False)

	def run(self):
		##交互获取
		waitinglist = pd.read_csv(self.connect_path + 'waiting.csv')
		gotlist = list(waitinglist['url'])
		gotlist = gotlist[:min(10, len(gotlist))]

		##多线程
		pool = Pool(processes = self.processes)
		result = pool.map(self.get_html, gotlist)
		pool.close()
		pool.join()
		for idx, html in enumerate(result):
			if html == 'Error':
				continue
			result[idx] = []
			html = html.decode('gbk')
			nt = '&nbsp'
			regex = '(?<=%s)(?:<.*?>)*([\s\S]*?)(?:<.*?>)*(?=%s)' #万能寻找两标识间字符
			c0 = usere(regex%('宗地标识：','宗地编号'), html)[0]
			c0 = usere('土地面积[\s\S]+?([\d.&nbsp;]+?)</span>', html)
			c1 = usere('原土地使用权人[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c2 = usere('现土地使用权人[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c3 = usere('土地使用权类型[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c4 = usere('土地利用状况[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c5 = usere('土地级别[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c6 = usere('转让价格[\s\S]+?([\d.&nbsp;]+?)</span>', html)
			c7 = usere('成交时间[\s\S]+?([\d:/\s&nbsp;]+?)</span>', html)
			c8 = usere('转让方式[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c9 = usere('所在行政区[\s\S]+?([\d&nbsp;]+?)</span>', html)
			c10 = usere('宗地编号[\s\S]+?([\d\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			c11 = usere('土地使用年限[\s\S]+?([\d.&nbsp;]+?)</span>', html)
			c12 = usere('土地用途[\s\S]+?([\u4e00-\u9fa5、&nbsp;]+?)</span>', html)
			for i in range(13):
				if eval('c%s'%(i)) == ['&nbsp;']:
					exec("c%s = ['']"%(i))
				self.content[i].extend(eval('c%s'%(i)))
		gotdf = pd.DataFrame({'url':gotlist})
		gotdf.to_csv(self.connect_path + 'got.csv')
		return result

	def get_proxy(self, count = 1):
		rand_num = random.randint(0, count)
		proxylist = pd.read_csv(self.connect_path + 'proxylist/proxies.csv')
		proxy = (proxylist['ip'][rand_num], proxylist['port'][rand_num])
		return proxy
	
	# def get_proxy(self, retry = 20):
	# 	'''
	# 	Randomly return an proxy
		
	# 	Parameters
	# 	----------
	# 	retry: int
	# 		retry times when exceptions occurs
			
	# 	Return
	# 	------
	# 	proxy: tuple
	# 		proxy[0] is ip, proxy[1] is port
	# 	'''
	# 	order = "d6854cac83b00b818845b10210432395"; 
	# 	apiUrl = "http://dynamic.goubanjia.com/dynamic/get/" + order + ".html";
	# 	for i in range(retry):
	# 		try:
	# 			res = requests.get(apiUrl, timeout = 10).text;
	# 			ip = usere('(.*):', res)
	# 			port = usere(':(.*)\n', res)
	# 			if len(ip) == 0:
	# 				print ('No proxies got')
	# 				raise
	# 			rand_num = random.randint(0, len(ip) - 1)
	# 			proxy = (ip[rand_num], port[rand_num])
	# 			return proxy
	# 		except Exception as e:
	# 			print (e)
	# 			time.sleep(5)