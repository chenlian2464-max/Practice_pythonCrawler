import random
import asyncio
import aiohttp
import logging
from pyquery import PyQuery as pq
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

TOTAL_PAGE = 1  # 总共爬取的页数
CONCURRENCY = 5 # 并发数
BASE_URL = 'https://books.toscrape.com/catalogue/{Page_url}'
INDEX_URL = 'https://books.toscrape.com/catalogue/page-{Number}.html'

semaphores = asyncio.Semaphore(CONCURRENCY)
session = None

MONGO_CONNECTION_STRING = "mongodb://localhost:27017"
MONGO_DATABASE_NAME = "books"
MONGO_CONNECTION_NAME = "books_connection"

client =AsyncIOMotorClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]
collection = db[MONGO_CONNECTION_NAME]

# 1.异步爬取所有列表页和详细页,声明task列表组成异步进行爬取任务
async def ScrapyAPI(url):
    async with semaphores:
        await asyncio.sleep(random.uniform(0.5,2))
        try:
            logging.info(f'Crapying {url}')
            async with session.get(url) as responce:
                return await responce.text()
        except aiohttp.ClientError as e:
                logging.error(f'Error {e} occurred when crawling {url}',exc_info = True)
async def Scrapy_indexPage(page):
     Index_url = INDEX_URL.format(Number = page)
     return await ScrapyAPI(Index_url)
# 2.所有列表页和详细页的数据解析，提取列表页的书籍链接与详细页的书名、价格、图片等信息
def parse_indexPage(html):
     doc = pq(html)
     book_urls = [a.attr['href'] for a in doc('.product_pod h3 a').items()]
     return  book_urls
def parse_bookPage(html):
    doc = pq(html)
    BookName = doc('.item.active img').attr('alt')
    Book_src = doc('.item.active img').attr('src').split('/')[2:]
    BookImage = 'https://books.toscrape.com/'+'/'.join(Book_src)
    Book_UPC = doc('.table.table-striped td').eq(0).text()
    BookType = doc('.table.table-striped td').eq(1).text()
    BookPrice = doc('.table.table-striped td').eq(2).text()
    BookAvailability = doc('.table.table-striped td').eq(5).text()
    BookDescription = doc('.product_page > p').text()
    BookInfo = {
        '书名': BookName,
        '价格': BookPrice,
        '图片': BookImage,
        'UPC': Book_UPC,
        '类型': BookType,
        '库存': BookAvailability,
        '简介': BookDescription
    }
    return BookInfo
# 3.异步的方式将爬取结果存储到MongoDB数据库中
async def save_to_mongo(BookInfo):
    logging.info(f'Saving BookInfo {BookInfo} to MongoDB')
    try:
        return await collection.update_one({
            '_id': BookInfo.get('UPC')
        },{
            '$set': BookInfo
        },upsert=True)
    except asyncio.CancelledError:
        logging.info('Scrapying cancelled')
        raise
    except TypeError as e:
        logging.error(f'Error {e} occurred when saving BookInfo to MongoDB',exc_info = True)
    except Exception as e:
        logging.error(f'Error {e} occurred when saving BookInfo to MongoDB',exc_info = True)
async def main():
    try:
        # one
        global session
        session = aiohttp.ClientSession()
        ScrapeIndexPage_tasks = [asyncio.ensure_future(Scrapy_indexPage(page)) for page in range(1,TOTAL_PAGE + 1)]
        results_html = await asyncio.gather(*ScrapeIndexPage_tasks)
        logging.info(f'Scrapying {len(results_html)} pages')
        # two
        Book_URL_lst = []
        for html in results_html:
            books_urls = parse_indexPage(html)
            for book_url in books_urls:
                Books_url = BASE_URL.format(Page_url = book_url)
                Book_URL_lst.append(Books_url)
        ScrapeBookPage_tasks = [asyncio.ensure_future(ScrapyAPI(book_url)) for book_url in Book_URL_lst]
        results_book_html = await asyncio.gather(*ScrapeBookPage_tasks)
        # three
        for book_html in results_book_html:
            bookData = parse_bookPage(book_html)
            await save_to_mongo(bookData)
            # logging.info(f'Scrapyed BookInfo {bookData}')
        logging.info(f'Scrapying {len(results_book_html)} pages')
    except Exception as e:
        logging.error(f'Error {e} occurred when Scrapying pages',exc_info = True)
    finally:
        client.close()
        await session.close()
        logging.info('Scrapying finished')
if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())