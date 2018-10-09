import re
import os
import sys
import time
import aiohttp
import asyncio
import aiofiles
import logging
from fake_useragent import UserAgent

start = time.time()
sem = asyncio.Semaphore(100)

class Spider(object):
    def __init__(self):
        self.page_url = "http://app.pearvideo.com/clt/jsp/v4/content.jsp"
        self.baseurl = "http://app.pearvideo.com/clt/jsp/v4/getCategoryConts.jsp?categoryId={cid}&hotPageidx=1"
        self.cids = [10, 1, 2, 9, 5, 8, 4, 3, 31, 6, 59]
        self.logger = logging.getLogger(__name__)
        self.app_headers = {
            "User-Agent": "LiVideoIOS/4.5.8 (iPhone; iOS 9.3.2; Scale/2.00)",
            'Host': 'app.pearvideo.com',
            'Cookie': 'PEAR_UUID=7159406F-FC15-4D5A-A827-315B7499835A;'
        }
        self.headers = {
            "User-Agent": UserAgent().random
        }
        self.filename = time.strftime("%Y-%m-%d", time.localtime(time.time())) + " " + "Pear_Video"

    def makedir(self):
        os.chdir("E:")
        if not os.path.isdir(self.filename):
            os.mkdir("E:\%s" % self.filename)

    async def run(self):
        self.makedir()
        urls = [self.baseurl.format(cid=cid) for cid in self.cids]
        async with sem:
            async with aiohttp.ClientSession() as session:
                for url in urls:
                    await self.get_contId(url, session)

    async def get_contId(self, url, session):
        async with session.get(url=url, headers=self.app_headers, timeout=10) as resp:
            if resp.status == 200:
                try:
                    text_json = await resp.json()
                    if text_json.get('resultCode') == '1' and text_json.get('resultMsg') == "success":
                        hotList = text_json.get('hotList')
                        contList = text_json.get('contList')
                        if hotList:
                            for hot in hotList:
                                contId = hot.get('contId')
                                await self.request_url(contId, session)
                        if contList:
                            for cont in contList:
                                contId = cont.get('contId')
                                await self.request_url(contId, session)
                        nextUrl = text_json.get('nextUrl')
                        if nextUrl:
                            await self.get_contId(nextUrl, session)
                except ConnectionError:
                    self.logger.debug("Request Error")

    async def request_url(self, id, session):
        formdata = {
            "contId": id
        }
        async with session.post(url=self.page_url, data=formdata, headers=self.app_headers, timeout=10) as resp:
            if resp.status == 200:
                try:
                    text_json = await resp.json()
                    if text_json.get('resultCode') == '1' and text_json.get('resultMsg') == "success":
                        videos = text_json.get('content').get('videos')
                        if videos:
                            video_name = text_json.get('content').get('name')
                            video_url = [video.get("url") for video in videos if "hd.mp4" in video.get("url")][0]
                            video_size = [video.get('fileSize') for video in videos if video.get('url') == video_url][0]
                            if video_url and video_name and video_size:
                                if video_url.endswith(".mp4"):
                                    if "|" or '"' or "." in video_name:
                                        video_name = video_name.replace("|", '').replace('"', '')
                                        await self.video_download(video_url, video_name, int(video_size), session)
                except Exception as e:
                    self.logger.debug(e)

    async def video_download(self, url, name, size, s):
        async with s.get(url, headers=self.headers, timeout=10) as resp:
            if resp.status == 200:
                chunk_size = 1024
                try:
                    async with aiofiles.open("E://%s//%s.mp4" % (self.filename, name), mode='wb') as f:
                        while True:
                            chunk = await resp.content.read(chunk_size)
                            if not chunk:
                                print('%s  %s.mp4 下载完成' % (name, '[文件大小]:%0.2f MB' % (size / chunk_size / 1024)))
                                break
                            await f.write(chunk)

                except Exception as e:
                    print(e)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    spider = Spider()
    task = asyncio.ensure_future(spider.run())
    loop.run_until_complete(task)
    end = time.time()
    print("耗时：", end - start)
















