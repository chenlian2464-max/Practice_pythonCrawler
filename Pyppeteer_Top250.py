import random
import logging
import asyncio
import json
import time
from os.path import exists
from os import makedirs
from pyppeteer import launch
from fake_useragent import UserAgent
from pyppeteer.errors import TimeoutError, ElementHandleError

logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
TIME_OUT = 10  # 超时时间
TOTAL_PAGE = 0 # 总页数
WINDOW_WIDTH, WINDOW_HEIGHT = 1920, 1080  # 窗口大小
HEADLESS = True  # 是否隐藏浏览器界面
INDEX_URL = 'https://movie.douban.com/top250?start={page}&filter='

RESULTS_DIR = 'results_top250'
exists(RESULTS_DIR) or makedirs(RESULTS_DIR)

browser,tab = None,None

async def init_browser():
    global browser,tab # 声明全局变量
    logging.info('正在初始化浏览器...')
    browser = await launch(
        executablePath='C:/Program Files/Google/Chrome/Application/chrome.exe',
        headless=HEADLESS,
        args=['--disable-infobars', '--window-size={},{}'.format(WINDOW_WIDTH, WINDOW_HEIGHT)]
    ) # 启动浏览器
    logging.info('浏览器初始化成功！')
    tab = await browser.newPage() # 打开新标签页
    await tab.setViewport({'width': WINDOW_WIDTH, 'height': WINDOW_HEIGHT}) # 设置视窗大小
    logging.info('标签页初始化成功！')

async def scrape_page(url,selector):
    # 随机等待时间
    await asyncio.sleep(random.uniform(1.5,3.5))
    logging.info('正在爬取{}...'.format(url))
    try:
        await tab.setRequestInterception(True) # 设置请求拦截器
        # async def on_request(req):
        #     headers = req.headers
        #     headers['Cookie'] = 'MyValuell="118218"; bid=r5FWkSnoRuU; push_noty_num=0; push_doumail_num=0; ap_v=0,6.0; dbsawcv1=MTc2MDYyNDI5N0AxYmJlMzYxNjc0MWYwYmY3YzRjODM4YzcwN2E0YTU2OTdjZTc4ZmI2MzE4YmQ1ODIyNjAyZjViNzNlNWUxNTQ3QGJiZGZiNDJiODI1MjkwYTdAMTQxYjM5ZmM4NDU4; dbcl2="256070325:SUgDVQNA9cU"; ck=_slM; frodotk_db="19f2549c16a4c7284dd6fdd443ee5578"'  # 添加自定义请求头
        #     headers['User-Agent'] = UserAgent().random
        #     await req.continue_({'headers': headers})
        # tab.on('request', on_request) # 设置请求头
        # await tab.goto(url) # 跳转到指定页面
        await tab.waitForSelector(selector, options = {'timeout':TIME_OUT*1000}) # 等待元素出现
    except TimeoutError:
        logging.error(f'超时！{url}', exc_info=True)

async def scrape_index_page(page):
    url = INDEX_URL.format(page = page*25)
    await scrape_page(url,'#content')

async def parse_index_page():
    return await tab.querySelectorAllEval('.hd a','nodes =>nodes.map(node => node.href)')

async def scrape_detail_page(url):
    await scrape_page(url,'h1')

async def parse_detail_page():
    title = await tab.querySelectorEval('h1','node => node.innerText.trim()')
    cover = await tab.querySelectorEval('#mainpic img','node => node.src')
    director = await tab.querySelectorEval('#info span.attrs a[rel="v:directedBy"]','node => node.textContent.trim()')
    actions = await tab.querySelectorAllEval('#info span.attrs a[rel="v:starring"]','nodes => nodes.map(node => node.textContent.trim())')
    genres = await tab.querySelectorAllEval('#info span[property="v:genre"]','nodes => nodes.map(node => node.textContent.trim())')
    runtime = await tab.querySelectorEval('#info span[property="v:runtime"]','node => node.textContent.trim()')
    release_date = await tab.querySelectorAllEval('#info span[property="v:initialReleaseDate"]','nodes => nodes.map(node => node.textContent.trim())')
    return {
        'title': title,
        'director': director,
        'actions': actions,
        'runtime': runtime,
        'genres': genres,
        'cover': cover,
        'release_date': release_date,
    }
async def save_date_to_json(data,name):
    json_name = name
    data_path = f'{RESULTS_DIR}/{json_name}.json'
    try:
        with open(data_path,'w',encoding='utf-8') as f:
            json.dump(data,f,ensure_ascii = False,indent = 4)
    except IOError as e:
        logging.error('文件保存失败:%s',e)
    except TypeError as e:
        logging.error('数据格式错误:%s',e)
    finally:
        logging.info(f'{json_name}保存成功！')
async def main():
    await init_browser()
    try:
        start_time = time.time()
        for page in range(0,TOTAL_PAGE + 1):
            await scrape_index_page(page)
            detail_urls = await parse_index_page()
            for detail_url in detail_urls:
                await scrape_detail_page(detail_url)
                detail_data = await parse_detail_page()
                await save_date_to_json(detail_data,detail_data['title'])
            logging.info(f'第{page+1}页共有{len(detail_urls)}部电影爬取完成！')
        end_time = time.time()
    except Exception as e:
        logging.error(f'爬取失败！{e}', exc_info=True)
    except ElementHandleError as e:
        logging.error(f'元素未找到！{e}', exc_info=True)
    finally:
        await browser.close()
        logging.info('浏览器已关闭！')
        logging.info(f'共耗时{end_time - start_time:.2f}秒！')

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())