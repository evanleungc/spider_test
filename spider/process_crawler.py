import time
import urllib
import threading
import multiprocessing
from mongo_cache import MongoCache
from mongo_queue import MongoQueue
from downloader import Downloader

SLEEP_TIME = 1
DEFAULT_AGENT = {}
DEFAULT_DELAY = 5
DEFAULT_RETRIES = 1
DEFAULT_TIMEOUT = 100
DEFAULT_PROXY_LIST = '/Users/apple/Desktop/connect/proxylist/proxies.csv'
DEFAULT_COOKIE = {}


def threaded_crawler(seed_url, delay=5, cache=None, scrape_callback=None,\
 user_agent='wswp', proxies=None, num_retries=1, max_threads=10, timeout=60):
    """Crawl using multiple threads
    """
    # the queue of URL's that still need to be crawled
    crawl_queue = MongoQueue()
    webpage_cache = MongoCache()
    # crawl_queue.clear()
    crawl_queue.push(seed_url)
    D = Downloader(delay=DEFAULT_DELAY, user_agent=DEFAULT_AGENT, proxies=DEFAULT_PROXY_LIST, \
            cookies = DEFAULT_COOKIE, num_retries=DEFAULT_RETRIES, timeout=DEFAULT_TIMEOUT, \
            opener=None, cache=MongoCache())

    def process_queue():
        while True:
            # keep track that are processing url
            try:
                url = crawl_queue.pop()
            except KeyError:
                # currently no urls to process
                break
            else:
                html = D(url)
                if scrape_callback:
                    try:
                        links = scrape_callback(url, html) or []
                    except Exception as e:
                        print ('Error in callback for: {}: {}'.format(url, e))
                    else:
                        for link in links:
                            # add this new link to queue
                            crawl_queue.push(normalize(seed_url, link))
            if (500 <= webpage_cache[url]['code'] < 600) | (webpage_cache[url]['code'] == -999):
                crawl_queue.reset(url)
            else:
                crawl_queue.complete(url)

    # wait for all download threads to finish
    threads = []
    while threads or crawl_queue:
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue.peek():
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True) # set daemon so main thread can exit when receives ctrl-c
            thread.start()
            threads.append(thread)
        time.sleep(SLEEP_TIME)


def process_crawler(args, **kwargs):
    num_cpus = int(multiprocessing.cpu_count() / 2)
    #pool = multiprocessing.Pool(processes=num_cpus)
    print ('Starting {} processes'.format(num_cpus))
    processes = []
    for i in range(num_cpus):
        p = multiprocessing.Process(target=threaded_crawler, args=[args], kwargs=kwargs)
        #parsed = pool.apply_async(threaded_link_crawler, args, kwargs)
        p.start()
        processes.append(p)
    # wait for processes to complete
    for p in processes:
        p.join()


def normalize(seed_url, link):
    """Normalize this URL by removing hash and adding domain
    """
    link, _ = urllib.parse.urldefrag(link) # remove hash to avoid duplicates
    return urllib.parse.urljoin(seed_url, link)