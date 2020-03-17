#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-12-31 17:07
# Author: turpure


import asyncio
from common.logger import logger
from common.tools import BaseCrawler, MsSQL, MySQL
from common.color import get_color_dict
from pyppeteer import launch

color_dict = get_color_dict()
my_sql = MySQL()


async def fetch(product_id, job_id):
    browser = await launch()
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
            '--blink-settings=imagesEnabled=false'
            # '--disable-infobars'
        ]})

    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36')
    url = 'https://www.vova.com/' + product_id + '?currency=HKD'
    # url = 'https://www.joom.com/en/products/5c2dc9dc6ecda80101beac3c'
    print(url)


    await page.goto(url,{'timeout':40000})

    print(await page.cookies())
    # html = await page.title()
    html = await page.content()
    print(html)
    # intercept = partial(intercept_response, job_id=job_id)
    # page.on('response', intercept)
    # await page.waitFor(10000)
    # await page.waitForNavigation({'timeout':60000})
    await browser.close()




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
                job = self.get_task('vova_job_list', block=True)
                print(job)
                if job:
                    job_info = job.split(',')
                    job_id, pro_id = job_info
                    asyncio.get_event_loop().run_until_complete(fetch(pro_id, job_id))
            except Exception as why:
                self.logger.info(f'fail to finish task cause of {why}')




if __name__ == '__main__':
    import time
    start = time.time()
    # worker = Crawler('mysql')
    # worker.run()
    pro_id = '1pc-Universal-Smart-Phone-Earphone-Bluetooth-Wireless-Stereo-GSN1562278508558793935411272-g5576985-m6657718'
    job_id = 'test'
    asyncio.get_event_loop().run_until_complete(fetch(pro_id, job_id))
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')