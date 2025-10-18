from fake_useragent import UserAgent
from typing import Optional
import requests
import logging
import random
import time
# 日志处理格式
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
def get_random_headers(referer:str) -> dict:
    """生成动态请求头"""
    return {
        "User-Agent": UserAgent().random, # 使用随机UA
        "Referer": f"{referer}",
    }

def scrape_page(url:str,Referer_url:str = None,retry:int = 3) -> Optional[requests.Response]:
    for attempt in range(retry,0,-1):
        try:
            delay = random.uniform(0.5,1.5) + (retry - attempt) * 2
            time.sleep(delay)
            response = requests.get(url,headers = get_random_headers(referer = Referer_url),timeout = (3.05,10)) # 连接/读取分开超时
            response.encoding = "UTF-8"
            if response.status_code == 200:
                logging.info('爬取 %s 完成',url)
                return response
            if response.status_code == 429:
                logging.warning(f'触发限流，剩余重试 {attempt-1}次')
                continue
            logging.error(f'无效状态码 {response.status_code} 于 {url}')
            return None
        except requests.Timeout:
            logging.warning(f'连接超时，剩余重试 {attempt-1}次')
        except requests.RequestException as e:
            logging.error(f'请求异常 {e} 于 {url}',exc_info=True)
            logging.error(f'达到最大重试次数 {retry} 于 {url}')
            return None
        finally:
            response.close() # 关闭连接