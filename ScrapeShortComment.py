from Scrape_api import scrape_page
from pyquery import PyQuery as pq
from os.path import exists
from os import makedirs
import logging
import json
""" 爬取短评数据,保存为json()数据格式 """
"""
    + 该项目严格遵守robots.txt协议
    + 采集数据仅用于学术研究
    - 实际应用需获得官方授权
"""

# 设置日志
logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# 设置防盗链
Referer_url = 'https://movie.douban.com/review/best/'
# 设置存储配置
RESULTS_DIR = 'results'
exists(RESULTS_DIR) or makedirs(RESULTS_DIR)
# 1.从原网页中提取name和cid
def get_Source_text(source_url,Referer_url):
    Response = scrape_page(source_url,Referer_url)
    return Response.text
def get_Source_data(Source_text):
    # 解析页面
    doc = pq(Source_text)
    divs = doc('.review-list.chart > div')
    if not divs:
        logging.error('not find divs')
        return [],[]
    names,cids = [],[]
    for div in divs.items():               
            names.append(div('.subject-img img').attr("title"))   # 电影名 
            cids.append(div.attr('data-cid'))                     # 评论id
    return names,cids
# 2.用cid拼接Ajax的url后获取json数据
def get_json_data(Ajax_url,Referer_url):
     return scrape_page(Ajax_url,Referer_url)
# 3.以name为名保存数据为json格式
def save_json_data(data,name):
    json_name = name
    data_path = f'{RESULTS_DIR}/{json_name}.json'
    try:
        with open(data_path,'w',encoding='utf-8') as f:
            json.dump(data,f,ensure_ascii = False,indent = 4)
    except IOError as e:
        logging.error('文件保存失败:%s',e)
    except TypeError as e:
        logging.error('数据格式错误:%s',e)
# 主函数
def main(start,end,step):
    try:
        for page in range(start,end,step):
            # 爬取列表前2页
            logging.info('正在爬取第%d页...',(page - start)//step + 1)
            source_url = f'https://movie.douban.com/review/best/?start={page}'
            Source_text = get_Source_text(source_url,Referer_url)
            # 爬取每一列表页的数据(name,cid)
            Source_names,Source_cids = get_Source_data(Source_text)
            # 爬取每一列表页中每一条评论的json数据
            for Source_Num in range(0,len(Source_cids)):
                Ajax_url = f'https://movie.douban.com/j/review/{Source_cids[Source_Num]}/full'
                Ajax_json = get_json_data(Ajax_url,source_url)
                json_data = Ajax_json.json()         
                film_name = Source_names[Source_Num] # 传递name作为json文件名
                save_json_data(json_data,film_name)
                logging.info('第%d页第%d条评论爬取完成！',(page - start)//step + 1,Source_Num+1)
    except Exception as e:
        logging.error('爬取失败:%s',e)
    logging.info('脚本爬取完成！')

if __name__ == '__main__':
    main(0,40,20)