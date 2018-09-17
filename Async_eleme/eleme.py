import time
import aiohttp
import asyncio
import aiomysql
import pymongo
from fake_useragent import UserAgent

items_urls = ['https://h5.ele.me/restapi/shopping/v3/restaurants?latitude=23.12468&longitude=113.3612&offset={0}&limit=8']

item_urls = []
failure_urls = []
retry_urls = []
retry_comment_urls = []
request_urls = []
stopping = False
offset = 0
num = 0
failure_time = 0
retry_time = 0

headers = {
    "user-agent": UserAgent().random,
    "referer": "https://h5.ele.me/"
}

start_time = time.time()
PROXY_POOL_URL = 'http://localhost:5555/random'
sem = asyncio.Semaphore(100)

async def get_proxy(s):
        try:
            async with s.get(PROXY_POOL_URL) as resp:
                if resp.status == 200:
                    proxy = await resp.text(encoding="utf-8")
                    print("使用代理：%s" % proxy)
                    return 'http://' + proxy
        except ConnectionError:
            return None

async def retry(s, url, loop):
    global retry_time
    while retry_time < 6:
        if len(retry_urls) == 0:
            await asyncio.sleep(0.5)
            continue

        async with s.get(url, headers=headers, proxy=await get_proxy(s)) as resp:
            if resp.status in [200, 201]:
                result = await resp.json()
                text_json = result.get('items')
                if text_json is not None:
                    await parse_page(text_json, loop, s)
                    break
            else:
                retry_time += 1
                print("retry url:%s,retry time:%s" % (url, retry_time))

    if retry_time >= 6:
        failure_urls.append(url)
        print("failure url:", url)

async def request_page(loop):
    async with sem:
        while not stopping:
            try:
                global num
                offset = num * 8
                num += 1
                print("Page:%s" % offset)
                async with aiohttp.ClientSession() as session:
                    url = items_urls[0].format(offset)
                    print("Request url:%s" % url)
                    async with session.get(url, headers=headers) as resp:
                        if resp.status in [200, 201]:
                            result = await resp.json()
                            text_json = result.get('items')
                            if len(text_json) > 0:
                                print(text_json)
                                request_urls.append(url)
                                await parse_page(text_json, loop, session)
                            else:
                                print(text_json)
                                print('there is no content in the next page')
                                break
                        else:
                            retry_urls.append(url)
                            await retry(session, url, loop)
                            print('failure url:%s,failure code:%s' % (url, resp.status))

            except Exception as e:
                print("[Info] Exception:", e)

async def parse_page(text_json, loop, s):
    for one in text_json:
        item = one.get('restaurant')
        authentic_id = item.get('authentic_id')
        shopid = item.get('id')
        shopname = item.get('name')
        rating = item.get('rating')
        recent_order_num = item.get('recent_order_num')
        opening_hours = item.get('opening_hours')[0] if item.get('opening_hours') else ''
        address = item.get('address')
        flavors_id = ' '.join([str(item.get('id')) for item in item.get('flavors')]) if item.get('flavors') else ''
        flavors_name = ' '.join([item.get('name') for item in item.get('flavors')]) if item.get('flavors') else ''
        minimum_order_amount = item.get('float_minimum_order_amount')
        delivery_fee = item.get('float_delivery_fee')
        distance = item.get('distance')
        order_lead_time = item.get('order_lead_time')
        support_tags = ' '.join([item.get('text') for item in item.get('support_tags')]) if item.get('support_tags') else ''
        recommend_reasons = ' '.join([item.get('name') for item in item.get('recommend_reasons')]) if item.get('recommend_reasons') else ''
        recommend = item.get('recommend').get('reason')
        description = item.get('description')
        promotion_info = item.get('promotion_info')
        phone = item.get('phone')
        folding_restaurant_brand = item.get('folding_restaurant_brand') if item.get('folding_restaurant_brand') else ''
        folding_restaurants_id = ' '.join([str(item.get('id')) for item in item.get('folding_restaurants')]) if item.get('folding_restaurant_brand') else ''
        folding_restaurants_name = ' '.join([item.get('name') for item in item.get('folding_restaurants')]) if item.get('folding_restaurant_brand') else ''
        latitude = item.get('latitude')
        longitude = item.get('longitude')
        crawl_time = time.strftime('%Y-%m-%d', time.localtime())

        data = (authentic_id, shopid, shopname, rating, recent_order_num, opening_hours, address,
                flavors_id, flavors_name, minimum_order_amount, delivery_fee, distance,
                order_lead_time, support_tags, recommend_reasons, recommend, description,
                promotion_info, phone, folding_restaurant_brand, folding_restaurants_id, folding_restaurants_name,
                latitude, longitude, crawl_time)
        await save_into_mysql(data, loop)
        await parse_comment(s, shopid)

async def parse_comment(s, shopid):
    url = 'https://h5.ele.me/restapi/ugc/v3/restaurants/{0}/ratings?has_content=true&tag_name=%E5%85%A8%E9%83%A8&offset=0&limit=100000'.format(shopid)
    async with s.get(url, headers=headers) as resp:
        if resp.status in [200, 201]:
            comment_list = await resp.json()
            rated_datetime = [item.get('rated_at') for item in comment_list]
            score = [item.get('rating') for item in comment_list]
            content = list(filter(None, [item.get('rating_text').strip() for item in comment_list]))
            crawl_time = time.strftime('%Y-%m-%d', time.localtime())
            data = {
                "rated_datetime": rated_datetime,
                "score": score,
                "content": content,
                "crawl_time": crawl_time
            }
            await save_into_mongodb(data)
        else:
            retry_comment_urls.append(url)
            await retry(s, url, loop)
            print('failure url:%s,failure code:%s' % (url, resp.status))

async def save_into_mongodb(data):
    print('正在写入MongoDB')
    client = pymongo.MongoClient(host='127.0.0.1', port=27017)
    elemedb = client['eleme']                           # 给数据库命名
    collection = elemedb['eleme_guangzhou_comment']     # 创建数据表
    collection.insert_one(data)
    print('写入MongoDB完成')

async def save_into_mysql(data, loop):
    pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                      user='root', password='104758993',
                                      db='spider', loop=loop, charset='utf8', autocommit=True)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            print('正在写入MySQL')
            insert_sql = """
                insert into async_guangzhou_eleme(authentic_id, shopid, shopname, rating, recent_order_num, opening_hours, address,
                                        flavors_id, flavors_name, minimum_order_amount, delivery_fee, distance,
                                        order_lead_time, support_tags, recommend_reasons, recommend, description,
                                        promotion_info, phone, folding_restaurant_brand, folding_restaurants_id, folding_restaurants_name,
                                        latitude, longitude, crawl_time)
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            await cur.execute(insert_sql, data)
            print('写入MySQL完成')
    pool.close()
    await pool.wait_closed()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(request_page(loop))
    loop.run_until_complete(task)
    loop.close()
    print("*"*20, "END INFO", "*"*20)
    print("total request url:", len(request_urls))
    print("retry url counts:%s" % len(retry_urls))
    print("failure url counts:%s" % len(failure_urls))
    print("Consumed time:", time.time() - start_time)