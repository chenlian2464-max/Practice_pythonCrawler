import time
import requests
import logging
import pymongo
import random
from bs4 import BeautifulSoup
from pyquery import PyQuery as pq
from fake_useragent import UserAgent
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

""" 爬取豆瓣Top250的电影数据，250条电影保存至MongoDB """

# 日志处理
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 爬取网址
BASE_URL = 'https://movie.douban.com/top250'
TOTAL_Page = 10 # 每25条数据一页，共10页
# 配置mongodb
MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
MONGO_DB_NAME = "douban"
MONGO_COLLECTION_NAME = "top250"
# 连接mongodb
@contextmanager
def get_mongo_collection():
    client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    try:
        yield client[MONGO_DB_NAME][MONGO_COLLECTION_NAME]
    finally:
        client.close()

# 增加动态headers
def get_random_headers():
    return {
        "User-Agent": UserAgent().edge,
        # "Accept-Encoding": "gzip, deflate, br, zstd",
        # "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Referer": "https://movie.douban.com/",
        "Cookie": f"bid={''.join(random.choices('abcdefghijklmnopqrstuvwxyz1234567890', k=11))};"
    }

# 1.用request爬取这个站点每一页的电影列表，获取每一个电影的详情页链接
def scrape_page(url,retry = 3):
    if retry <= 0:
        logging.error("Max retries reached")
        return None
    # 先休眠再请求（更符合人类操作）
    time.sleep(random.uniform(3, 5)) # 随机延迟
    logging.info('正在爬取 %s...',url)
    try:
        response = requests.get(url,headers = get_random_headers(),timeout = 10)  # 增加超时限制
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response.text
        elif response.status_code == 429:
            delay = (4 - retry) ** 2 * 15
            time.sleep(delay)  # 遇到429错误时延长等待
            return scrape_page(url,retry-1)  
        else:
            logging.error('get invalid response code %s whiling scraping %s',response.status_code,url)
            return None
    except requests.RequestException:
        logging.error('error occurred while scraping %s',url,exc_info=True)
        time.sleep(10)  # 异常时延长等待
        return scrape_page(url,retry-1)  # 遇到网络错误时延长等待
def scrape_index(page):
    index_url = f'{BASE_URL}?start={page*25}&filter='
    return scrape_page(index_url)
def parse_index(html):
    doc = pq(html)
    links = doc('.hd a')
    for link in links.items():
        href = link.attr('href')
        details_url = href
        logging.info('get details url %s',details_url)
        yield details_url

# 2.用pyquery和BeautifulSoup解析每一个电影的详情页，获取电影的名称、导演、主演、类型、评分、评价人数、上映日期、片长、别名、剧情简介、海报图片链接
def scrape_detail(url): 
    return scrape_page(url)
def parse_detail(html,url):
    doc = pq(html)
    soup = BeautifulSoup(html,"html.parser")

    douban_id = url.split('/')[-2]
     # 豆瓣ID

    cover = doc('#mainpic img').attr('src')
     # 电影图片

    name = doc('#content h1').text()
     # 电影名

    director = doc('#info .attrs').eq(0).text()
     # 导演

    writer = doc('#info .attrs').eq(1).text()
     # 编剧

    actors = doc('#info .attrs').eq(2).text()
     # 演员

    genres = [span.text for span in doc('#info span[property="v:genre"]')]
     # 类型

    country_elem = soup.find("span",class_="pl",string='制片国家/地区:')
    country =country_elem.next_sibling.strip() if country_elem else None
     # 地区
    
    language_elem = soup.find('span',class_='pl',string='语言:')
    language =language_elem.next_sibling.strip() if language_elem else None
     # 语言

    release_date = doc('#info span[property="v:initialReleaseDate"]').text()
     # 上映日期

    runtime = doc('#info span[property="v:runtime"]').text()
     # 片长

    also_known_as_elem = soup.find('span',class_='pl',string='又名:')
    also_known_as = also_known_as_elem.next_sibling.strip().split('/') if also_known_as_elem else []
     # 又名

    imdb_elem = soup.find('span',class_='pl',string='IMDb:')
    imdb = imdb_elem.next_sibling.strip() if imdb_elem else None
     # IMDb评分

    reviews = doc('span[property="v:summary"]').text()
     # 影评
    return {
        '_id': douban_id,  # MongoDB自动使用_id字段作为主键
        'cover':cover,
        'name':name,
        'director':director,
        'writer':writer,
        'actors':actors,
        'genres':genres,
        'country':country,
        'language':language,
        'release_date':release_date,
        'runtime':runtime,
        'also_known_as':also_known_as,
        'imdb':imdb,
        'reviews':reviews
    }

# 3.将获取到的信息存储到mongodb数据库中
def save_to_mongo(data):
    logging.info('start save data to douban top250')
    try:
        with get_mongo_collection() as collection:
            collection.update_one({
                '_id':data.get('_id')
            },{
                '$set':data
            },upsert = True)
            logging.info("数据保存成功 %s",data)
    except Exception as e:
        logging.error("保存数据到MongoDB失败: %s", e, exc_info=True)

# 4.使用多进程或协程提高爬取速度
def process_page(page):
    logging.info('start scraping douban top250')
    index_html = scrape_index(page)
    detail_urls = parse_index(index_html)
    logging.info('start scraping details')
    for url in detail_urls:
        detail_html = scrape_detail(url)
        data = parse_detail(detail_html,url)
        logging.info('get detail data %s',data)
        logging.info('save data to mongo')
        save_to_mongo(data)
        logging.info("data saved successfully %s",data)

if __name__ == '__main__':
    # 控制并发数和请求间隔
    MAX_WORKERS = 1  # 并发线程数（建议不超过2）
    REQUEST_INTERVAL = 8  # 每个线程的最小间隔
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for page in range(0, TOTAL_Page):
            executor.submit(process_page, page)
            time.sleep(REQUEST_INTERVAL / MAX_WORKERS)  # 控制总体请求速率
    logging.info("all data saved successfully")
