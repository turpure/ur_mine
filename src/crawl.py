#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-12-31 17:07
# Author: turpure


import asyncio
import re
import time
from functools import partial
from common.logger import logger
from common.tools import BaseCrawler, MsSQL, MySQL
from common.color import get_color_dict
from pyppeteer import launch

color_dict = get_color_dict()
my_sql = MySQL()


async def fetch(product_id, job_id):
    browser = await launch({
        'headless': True,
        'args': [
            '--disable-extensions',
            '--hide-scrollbars',
            '--disable-bundled-ppapi-flash',
            '--mute-audio',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--disable-gpu',
        ]})

    page = await browser.newPage()
    await page.goto(f'https://www.joom.com/en/products/{product_id}')
    intercept = partial(intercept_response, job_id=job_id)
    # page.on('response', intercept)

    #等待标题出现
    await page.waitForXPath('//h1[@itemprop="name"]')
    # await page.waitFor(10000)
    await browser.close()


async def intercept_response(res, job_id):
    resourceType = res.request.resourceType
    if resourceType in ['xhr']:
        url = res.url
        if re.match(r'.*/products/[a-z0-9\-]+\?currency', url):
            try:
                ret = await res.json()
                if ret:
                    rows = parse(ret)
                    # return rows
                    my_sql.insert(rows, job_id=job_id)
            except Exception as why:
                logger.info(f'fail to fetch response cause of {why}')


def parse(data):
    wanted_info = dict()
    main_info = dict()
    if data is None:
        yield wanted_info
        return

    pro_info = data['payload']
    try:
        tags_info = pro_info['nameExt']['tags']
        tags = ','.join([name['nameEng'] for name in tags_info])
    except:
        tags = ''
    main_info['tags'] = tags
    extra_images = pro_info.get('gallery', '')
    main_info['proId'] = pro_info.get('id', '')
    main_info['description'] = pro_info.get('engDescription', '')
    main_info['proName'] = pro_info.get('engName', '')
    main_info['categoryId'] = pro_info.get('categoryId', '')
    main_info['mainImage'] = pro_info['lite']['mainImage']['images'][-1]['url']
    for image in extra_images:
        main_info['extra_image' + str(extra_images.index(image))] = image['payload']['images'][-1]['url']
    for i in range(0, 11 - len(extra_images)):
        main_info['extra_image' + str(11 - i - 1)] = ''
    pro_variants = pro_info.get('variants', '')

    for var in pro_variants:
        variants = dict()
        try:
            try:
                variants['color'] = color_dict['#' + var['colors'][0]['rgb']]
            except:
                variants['color'] = var['colors'][0]['rgb']
        except:
            variants['color'] = ''
        variants['proSize'] = var.get('size', '')
        variants['msrPrice'] = var.get('msrPrice', 0)
        variants['shipping'] = var['shipping']['price']
        variants['shippingTime'] = '-'.join([str(var['shipping']['minDays']), str(var['shipping']['maxDays'])])
        variants['price'] = var['price']
        variants['shippingWeight'] = var.get('shippingWeight', 0)
        try:
            variants['varMainImage'] = var['mainImage']['images'][-1]['url']
        except:
            variants['varMainImage'] = ''
        # variants['quantity'] = var['inventory']
        variants['quantity'] = 100000
        wanted_info = dict(main_info, **variants)
        yield wanted_info

class Crawler(BaseCrawler):

    def __init__(self, db_type='mysql'):
        self.logger = logger
        if db_type == 'mssql':
            self.data_base = MsSQL()
        else:
            self.data_base = MySQL()

        super(Crawler, self).__init__()



    def get_task(self, queue_name, block=True):
        if block:
            task = self.redis.blpop(queue_name, timeout=10)
            task = task[1]
        else:
            task = self.redis.lpop(queue_name)
            if not task:
                time.sleep(1)
        return task

    def run(self):
        while 1:
            try:
                job = self.get_task('job_list', block=True)
                if job:
                    # global product_json
                    job_info = job.split(',')
                    job_id, pro_id = job_info
                    asyncio.get_event_loop().run_until_complete(fetch(pro_id, job_id))
                    # rows = self.parse(product_json)
                    # product_json = ''
                    # self.data_base.insert(rows, job_id)
            except Exception as why:
                self.logger.info(f'fail to finish task cause of {why}')


if __name__ == '__main__':
    worker = Crawler('mysql')
    worker.run()
    # pro_id = '1521169399436767973-209-1-26193-3280917902'
    # job_id = 'test'
    # asyncio.get_event_loop().run_until_complete(fetch(pro_id, job_id))