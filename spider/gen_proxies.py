import re
import time
import requests
import pandas as pd
def usere(regex, getcontent): #定义使用正则表达式的函数
    pattern = re.compile(regex)
    content = re.findall(pattern, getcontent)
    return content
def get_proxy(retry = 10):
	'''
	Randomly return an proxy
	
	Parameters
	----------
	retry: int
		retry times when exceptions occurs
		
	Return
	------
	proxy: tuple
		proxy[0] is ip, proxy[1] is port
	'''
	path = '/Users/apple/Desktop/connect/proxylist/'
	order = "d6854cac83b00b818845b10210432395"; 
	apiUrl = "http://dynamic.goubanjia.com/dynamic/get/" + order + ".html";
	for i in range(retry):
		try:
			res = requests.get(apiUrl, timeout = 10).text;
			ip = usere('(.*):', res)
			port = usere(':(.*)\n', res)
			if len(ip) == 0:
				print ('No proxies got')
				raise
			proxydf = pd.DataFrame({'ip': ip, 'port': port})
			proxydf.to_csv(path + 'proxies.csv', index = False)
		except Exception as e:
			print (ip, port)
			time.sleep(5)

while 1:
	get_proxy()
	time.sleep(2)