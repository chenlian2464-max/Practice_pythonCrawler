from Crypto.Cipher import AES
import requests
import aiofiles
import asyncio
import aiohttp
import os
import logging

# 爬取虎课网中的公开视频课 #

# 日志配置
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# 1.网页获取m3u8文件
def download_m3u8(url_m3u8):
    
    responce = requests.get(url_m3u8,headers={
    "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
})
    if responce.status_code == 200:
        # print(responce.text)
        with open("All_ts.m3u8.txt",mode="wb") as file:
            file.write(responce.content)
    else:
        print("无法获取！")
    responce.close()

# 2.在m3u8中下载ts文件(异步下载)
async def download_ts(url_m3u8,name,session):
    try:
        async with session.get(url_m3u8) as responce:
            if responce.status == 200:
                content = await responce.read()
                async with aiofiles.open(f"video/{name}",mode="wb") as file:
                    await file.write(content)
                    print(f"{name}下载完毕！")
            else:
                print(f"{name} 下载失败，状态码：{responce.status}")
    except:
         print(f"{name} 下载出错!")
async def aio_download_ts(url_ts):
    tasks = []
    async with aiohttp.ClientSession() as session:
        async with aiofiles.open("All_ts.m3u8.txt",mode="r",encoding="utf-8") as File:
            async for i in File:
                if i.startswith('#'):
                    continue
                i = i.strip()               
                url_m3u8 = url_ts + i       
                i_one = i.split('?')[0]
                i_ts = i_one.split('/')[-1] 
                task = asyncio.create_task(download_ts(url_m3u8,i_ts,session))
                tasks.append(task)
            await asyncio.wait(tasks)

# 3.将ts文件合并成一个mp4文件
# (1).首先需要解密，才能合并(异步解密)
async def decrypt_ts(encrypted_ts_path,output_path,get_key):
        IV = bytes.fromhex("69066a5d2f1c3a2ebd9cb81b41d7470d")
        async with aiofiles.open(encrypted_ts_path,"rb") as file:
            data = await file.read()

            cipher = AES.new(get_key,AES.MODE_CBC,iv = IV)
            decrypt_date = cipher.decrypt(data)

        async with aiofiles.open(output_path,"wb") as file_output:
            await file_output.write(decrypt_date)
async def aio_decrypt_ts(get_key):
    tasks = []
    async with aiofiles.open("All_ts.m3u8.txt",mode="r",encoding="utf-8") as file:
        async for i in file:
            if i.startswith('#'):
                continue
            i = i.strip()                   
            i_one = i.split('?')[0]
            i_ts = i_one.split('/')[-1]
            task = asyncio.create_task(decrypt_ts(f"video/{i_ts}",f"video_decrypt/dec_{i_ts}",get_key))
            tasks.append(task)
        await asyncio.wait(tasks)

        print(f"解密完成！")
# (2).将已解密的ts文件都合并成mp4
def find_path_ts():
    lst = []
    with open("All_ts.m3u8.txt",mode = "r",encoding = "utf-8") as file:
        for line in file:
            line = line.strip()
            if line.startswith('#'):
                continue
            i_one = line.split('?')[0]
            i_ts = i_one.split('/')[-1]
            ts_path = f'video_decrypt/dec_{i_ts}'
            Merge_ts(ts_path)
            break   
def Merge_ts(ts_files, output_file="output.mp4"):
    if not ts_files:
        print("没有找到TS文件")
        return False
    
    lst = []
    lst.append(ts_files)

    ts_files_str = "+".join(lst)
    command = f'copy /b {ts_files_str} "{output_file}"'

    result = os.system(command)

    if result == 0:
        print(f"合并成功，输出文件: {output_file} ")
        return True
    else:
        print(f"合并失败，命令返回码: {result}")
        return False

def main():
    pass

if __name__ == "__main__":
   url_ts = 'https://m3u8.huke88.com/'
   url_m3u8 = 'https://m3u8.huke88.com/video/hls/v_1/2024-09-20/06678EC2-BD50-49ED-0DFC-AA1FA4709816.m3u8?pm3u8/0/deadline/1758595981&e=1758556381&token=HUwgVvJnrW6fXOzqd_myfnE3FFoFLWJnNktg7ThD:eW7iL41RShG7J6MEDbttE_nbbx0='
   url_key = 'https://asyn.huke88.com/video/decrypt'
   # asyncio.run(aio_download_ts(url_ts))
#    get_key = requests.get(url_key,headers={
#     "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36 Edg/140.0.0.0"
# }).content
#    asyncio.run(aio_decrypt_ts(get_key))
   # download_m3u8(url_m3u8)
   # find_ts()
   main()