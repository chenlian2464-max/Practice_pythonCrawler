import time
import json
import random
import logging
from os import makedirs
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By
from selenium.webdriver import ChromeOptions
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
TIME_OUT = 10  # 超时时间
TOTAL_PAGE = 0 # 总页数

RESULTS_DIR = 'Top250_jsons'
makedirs(RESULTS_DIR,exist_ok = True)

browser = Chrome()
wait = WebDriverWait(browser,TIME_OUT)

# 将webdriver属性设置为false，防止selenium自动化框架检测到webdriver并报错
option = ChromeOptions()
option.add_experimental_option('excludeSwitches', ['enable-automation'])
option.add_experimental_option('useAutomationExtension', False)
browser = Chrome(options = option)
browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
    'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'
})

# 1.遍历列表页获取详细页链接
def  Scrapy_page(url,condition,locator):
    logging.info(f'scrapying...{url}')
    time.sleep(random.uniform(3,5))  # 随机等待时间
    try:
        browser.get(url)
        wait.until(condition(locator))
    except TimeoutException:
        logging.error('timeout occurred while scrapying...')
    except Exception:
        logging.error('error occurred while scrapying...')
def Scrapy_index(page):
    INDEX_URL = f'https://movie.douban.com/top250?start={page * 25}&filter='
    Scrapy_page(INDEX_URL,condition = EC.visibility_of_all_elements_located,locator = (By.CSS_SELECTOR,'.hd a'))
def parse_index():
    lst_urls =[]
    elements = browser.find_elements(By.CSS_SELECTOR,'.hd a')
    for element in elements:
        detail_url = element.get_attribute('href')
        lst_urls.append(detail_url)
    return lst_urls
# 2.遍历详细页获取电影信息
def Scrapy_detail(url):
    Scrapy_page(url,condition = EC.visibility_of_element_located,locator = (By.CSS_SELECTOR,'.info span'))
def parse_detail():
    url = browser.current_url
    info_div = browser.find_element(By.ID, 'info')
    title = info_div.find_element(By.XPATH,'//*[@id="content"]/h1').text
    director = info_div.find_element(By.XPATH,'//*[@id="info"]/span[1]/span[2]').text
    writers = [a.text for a in info_div.find_elements(By.XPATH,'//*[@id="info"]/span[2]/span[2]')]
    actors = [a.text for a in info_div.find_elements(By.XPATH,'//*[@id="info"]/span[3]')]
    img_url = info_div.find_element(By.XPATH,'//*[@id="mainpic"]/a/img').get_attribute('src')
    genres = [span.text for span in info_div.find_elements(By.XPATH,'//*[@id="info"]/span[@property="v:genre"]')]
    release_dates = [span.text for span in info_div.find_elements(By.XPATH,'//*[@id="info"]/span[@property="v:initialReleaseDate"]')]
    runtime = info_div.find_element(By.XPATH,'//*[@id="info"]/span[@property="v:runtime"]').text
    all_lines = info_div.text.split('\n')
    for line in all_lines:
        if '制片国家/地区' in line:
            country = line.split(':', 1)[1].strip()
        if '语言' in line:
            language = line.split(':',1)[1].strip()
        if 'IMDb' in line:
            imdb = line.split(':',1)[1].strip()
    movie_info = {
    'url': url,
    '电影名': title,
    '导演': director,
    '编剧': writers,
    '主演': actors,
    '类型': genres,
    '图片': img_url,
    '制片国家/地区': country,
    '语言': language,
    '上映日期': release_dates,
    '片长': runtime,
    'IMDb': imdb
}
    return movie_info
# 3.保存电影信息
def save_data_json(data):
    filename = f'{RESULTS_DIR}/{data["电影名"]}.json'
    try:
        with open(filename,'w',encoding = 'utf-8') as f:
            json.dump(data,f,ensure_ascii = False,indent = 2)
        logging.info(f'saved data to {filename}')
    except FileNotFoundError:
        logging.error(f'file {filename} not found')
    except IOError:
        logging.error(f'error occurred while writing to {filename}')
    except TypeError:
        logging.error(f'data type error while writing to {filename}')
    except Exception as e:
        logging.error(f'error occurred while saving data to {filename} %s ',e,exc_info = True)

def main():
    try:
        for page in range(0,TOTAL_PAGE + 1):
            logging.info(f'scrapying page {page + 1}')
            Scrapy_index(page)
            detail_urls = parse_index()
            # logging.info(f'finished scrapying page {page} and get {detail_urls} detail urls')
            for detail_url in detail_urls:
                Scrapy_detail(detail_url)
                datails_info = parse_detail()
                save_data_json(datails_info)
                # logging.info(f'finished scrapying page {page + 1} and get datails_info: {datails_info} ')
        logging.info('finished scrapying')
    except Exception as e:
        logging.error('error occurred while scrapying %s ',e,exc_info = True)
    finally:
        browser.close()
    pass
if __name__ == '__main__':
    main()