import urllib
import requests
import random
import time
from datetime import datetime, timedelta
import socket
import pandas as pd
import os


DEFAULT_AGENT = {}
DEFAULT_DELAY = 5
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 10
DEFAULT_PROXY_LIST = '/Users/apple/Desktop/connect/proxylist/proxies.csv'
DEFAULT_COOKIE = {}


class Downloader:
    def __init__(self, delay=DEFAULT_DELAY, user_agent=DEFAULT_AGENT, proxies=DEFAULT_PROXY_LIST, \
            cookies = DEFAULT_COOKIE, num_retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT, opener=None, cache=None):
        socket.setdefaulttimeout(timeout)
        self.timeout = timeout
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.cookies = DEFAULT_COOKIE
        self.num_retries = num_retries
        self.opener = opener
        self.cache = cache


    def __call__(self, url):
        result = None
        if self.cache:
            try:
                result = self.cache[url]
            except KeyError:
                # url is not available in cache 
                pass
            else:
                if self.num_retries > 0 and ((500 <= result['code'] < 600) | (result['code'] == -999)):
                    # server error so ignore result from cache and re-download
                    result = None
        if result is None:
            # result was not loaded from cache so still need to download
            self.throttle.wait(url)
            proxy = self.fetch_proxy() if self.proxies else None
            headers = self.user_agent
            cookies = self.cookies
            result = self.download(url, headers = headers, proxy=proxy, cookies = cookies, num_retries=self.num_retries)
            if self.cache:
                # save result to cache
                self.cache[url] = result
        return result['html']

    def fetch_proxy(self):
        proxylist = pd.read_csv(self.proxies)
        rand_num = random.randint(0, len(proxylist) - 1)
        proxy = (proxylist['ip'][rand_num], proxylist['port'][rand_num])
        ip, port = proxy
        proxy = {'http':'http://%s:%s'%(ip, port)}
        return proxy


    def download(self, url, headers, proxy, cookies, num_retries, data=None):
        print ('Downloading:', url)
        try:
            response = requests.get(url, timeout = self.timeout, proxies = proxy, headers = headers, cookies = cookies)
            html = response.content
            code = response.status_code
        except Exception as e:
            print ('Download error:', str(e))
            html = ''
            if hasattr(e, 'code'):
                code = e.code
                if num_retries > 0 and 500 <= code < 600:
                    # retry 5XX HTTP errors
                    return self.download(url, headers, proxy, cookies, num_retries-1, data)
            else:
                code = -999
        return {'html': html, 'code': code}


class Throttle:
    """Throttle downloading by sleeping between requests to same domain
    """
    def __init__(self, delay):
        # amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}
        
    def wait(self, url):
        """Delay if have accessed this domain recently
        """
        domain = urllib.parse.urlsplit(url).netloc
        last_accessed = self.domains.get(domain)
        if self.delay > 0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.now() - last_accessed).seconds
            if sleep_secs > 0:
                time.sleep(sleep_secs)
        self.domains[domain] = datetime.now()