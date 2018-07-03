from selenium import webdriver
import pandas as pd
from mongo_queue import MongoQueue
from datetime import datetime
import calendar
import re
import time

CONNECT_PATH = '/Users/apple/Desktop/connect/'

def usere(regex, getcontent): #定义使用正则表达式的函数
    pattern = re.compile(regex)
    content = re.findall(pattern, getcontent)
    return content

#抵押的Key
# startkey = '//*[@id="TAB_queryDateItem_291_1"]'
# endkey = '//*[@id="TAB_queryDateItem_291_2"]'
# dateclick = '//*[@id="TAB_QueryConditionItem291"]'
# dyrclick = '//*[@id="TAB_queryTextItem_82"]'
# dyr = '//*[@id="TAB_queryTextItem_82"]'
# tdytclick = '//*[@id="TAB_QueryConditionItem282"]'
# tdyt = '//*[@id="TAB_queryTblEnumItem_282"]'
# searchkey = '//*[@id="TAB_QueryButtonControl"]'

#转让的Key
startkey = '//*[@id="TAB_queryDateItem_277_1"]'
endkey = '//*[@id="TAB_queryDateItem_277_2"]'
dateclick = '//*[@id="TAB_QueryConditionItem277"]'
userkey = '//*[@id="TAB_queryTextItem_275"]'
userclick = '//*[@id="TAB_QueryConditionItem275"]'
searchclick = '//*[@id="TAB_QueryButtonControl"]'

crawl_queue = MongoQueue()
browser = webdriver.Chrome()
browser.get(url)
browser.find_element_by_xpath(dateclick).click()
browser.find_element_by_xpath(userclick).click()

datelist = []
for year in range(2009, 2017):
	for month in range(1, 13):
		startday, endday = calendar.monthrange(year, month)
		datelist.append([str(year) + '-' + str(month) + '-' + str(1),\
			str(year) + '-' + str(month) + '-' + str(endday)])
for date in datelist:
	returnlist = list(pd.read_csv(CONNECT_PATH + 'waiting.csv')['url'])
	browser.find_element_by_xpath(startkey).clear()
	browser.find_element_by_xpath(endkey).clear()
	browser.find_element_by_xpath(userkey).clear()
	browser.find_element_by_xpath(startkey).send_keys(date[0])
	browser.find_element_by_xpath(endkey).send_keys(date[1])
	browser.find_element_by_xpath(userkey).send_keys('公司')
	browser.find_element_by_xpath(searchclick).click()
	html = browser.page_source
	retlist = usere('<a href="(default.aspx?.+?)" target', html)
	retlist = ['http://www.landchina.com/' + i for i in retlist]
	retlist = [i.replace('&amp;', '&') for i in retlist]
	returnlist.extend(retlist)
	for i in retlist:
		crawl_queue.push(i)
	pagenum = int(usere('共(\d+?)页', html)[0])
	for i in range(pagenum):
		browser.find_element_by_link_text('下页').click()
		html = browser.page_source
		retlist = usere('<a href="(default.aspx?.+?)" target', html)
		retlist = ['http://www.landchina.com/' + i for i in retlist]
		retlist = [i.replace('&amp;', '&') for i in retlist]
		for i in retlist:
			crawl_queue.push(i)
		returnlist.extend(retlist)
		returndf = pd.DataFrame({'url':returnlist})
		returndf.to_csv(CONNECT_PATH + 'waiting.csv', index = False)