#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-01-02 10:20
# Author: turpure

import pymssql
import pymysql
import redis
from config.dev import mysql, mssql, rd
from common.logger import logger


class MsSQL(object):
    """context connection of mssql"""

    def __init__(self):
        db = mssql
        con = pymssql.connect(
            server=db['server'], user=db['user'],
            charset="utf8", password=db['password'],
            database=db['database'], port=db['port'])
        self.con = con

    def connection(self):
        return self.con

    def __enter__(self):
        return self.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.con.close()
        finally:
            self.con.close()

    def insert(self, rows, job_id):
        insert_sql = ("insert oa_data_mine_detail"
                      "(mid,parentId,proName,description,"
                      "tags,childId,color,proSize,quantity,"
                      "price,msrPrice,shipping,shippingWeight,"
                      "shippingTime,varMainImage,extra_image0,"
                      "extra_image1,extra_image2,extra_image3,"
                      "extra_image4,extra_image5,extra_image6,"
                      "extra_image7,extra_image8,extra_image9,"
                      "extra_image10,mainImage"
                      ") "
                      "values( %s,%s,%s,%s,%s,%s,%s,%s,"
                      "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                      "%s,%s,%s,%s,%s,%s,%s,%s)")
        update_sql = "update oa_data_mine set progress=%s where id=%s"
        code_sql = "select goodsCode from oa_data_mine where id=%s"
        main_image_sql = "update oa_data_mine set mainImage=%s where id=%s"
        con = self.connection()
        cur = con.cursor()
        try:
            cur.execute(code_sql, (job_id,))
            code_ret = cur.fetchone()
            code = code_ret[0]
            index = 1
            for row in rows:
                row['mid'] = job_id
                row['parentId'] = code
                row['childId'] = code + '_' + '0' * (2 - len(str(index))) + str(index)
                index += 1
                cur.execute(main_image_sql, (row['mainImage'], job_id))
                cur.execute(insert_sql,
                            (row['mid'], row['parentId'], row['proName'], row['description'],
                             row['tags'], row['childId'], row['color'], row['proSize'], row['quantity'],
                             float(row['price']), float(row['msrPrice']), row['shipping'], float(row['shippingWeight']),
                             row['shippingTime'],
                             row['varMainImage'], row['extra_image0'], row['extra_image1'], row['extra_image2'],
                             row['extra_image3'], row['extra_image4'], row['extra_image5'],
                             row['extra_image6'], row['extra_image7'], row['extra_image8'],
                             row['extra_image9'], row['extra_image10'], row['mainImage']))

            cur.execute(update_sql, (u'采集成功', job_id))
            con.commit()
            logger.info('fetching %s' % job_id)
        except Exception as why:
            logger.error('%s while fetching %s' % (why, job_id))
            cur.execute(update_sql, (u'采集失败', job_id))
            con.commit()


class MySQL(object):
    """context connection of mssql"""

    def __init__(self):
        db = mysql
        con = pymysql.connect(
            host=db['host'], user=db['user'],
            password=db['password'], charset=db['charset'],
            database=db['database'])
        self.con = con

    def connection(self):
        return self.con

    def __enter__(self):
        return self.connection()

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.con.close()
        finally:
            self.con.close()

    def insert(self, rows, job_id):
        insert_sql = ("insert oa_dataMineDetail"
                      "(mid,parentId,proName,description,"
                      "tags,childId,color,proSize,quantity,"
                      "price,msrPrice,shipping,shippingWeight,"
                      "shippingTime,varMainImage,extraImage0,"
                      "extraImage1,extraImage2,extraImage3,"
                      "extraImage4,extraImage5,extraImage6,"
                      "extraImage7,extraImage8,extraImage9,"
                      "extraImage10,mainImage"
                      ") "
                      "values( %s,%s,%s,%s,%s,%s,%s,%s,"
                      "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                      "%s,%s,%s,%s,%s,%s,%s,%s)")
        update_sql = "update oa_dataMine set progress=%s where id=%s"
        code_sql = "select goodsCode from oa_dataMine where id=%s"
        main_image_sql = "update oa_dataMine set mainImage=%s where id=%s"
        is_done_sql = 'select progress from oa_dataMine where id= %s'
        con = self.connection()
        cur = con.cursor()
        try:
            cur.execute(is_done_sql, (job_id,))
            is_done_ret = cur.fetchone()
            if is_done_ret[0] != '待采集':
                return
            cur.execute(code_sql, (job_id,))
            code_ret = cur.fetchone()
            code = code_ret[0]
            index = 1
            for row in rows:
                row['mid'] = job_id
                row['parentId'] = code
                row['childId'] = code + '_' + '0' * (2 - len(str(index))) + str(index)
                index += 1
                cur.execute(main_image_sql, (row['mainImage'], job_id))
                cur.execute(insert_sql,
                            (row['mid'], row['parentId'], row['proName'], row['description'],
                             row['tags'], row['childId'], row['color'], row['proSize'], row['quantity'],
                             float(row['price']), float(row['msrPrice']), row['shipping'], float(row['shippingWeight']),
                             row['shippingTime'],
                             row['varMainImage'], row['extra_image0'], row['extra_image1'], row['extra_image2'],
                             row['extra_image3'], row['extra_image4'], row['extra_image5'],
                             row['extra_image6'], row['extra_image7'], row['extra_image8'],
                             row['extra_image9'], row['extra_image10'], row['mainImage']))

            cur.execute(update_sql, (u'采集成功', job_id))
            con.commit()
            logger.info('fetching %s' % job_id)
            print(f'success to fetch {job_id}')
        except Exception as why:
            logger.error('%s while fetching %s' % (why, job_id))
            print(f'failed to fetch {job_id}')
            cur.execute(update_sql, (u'采集失败', job_id))
            con.commit()


class BaseCrawler(object):
    """ fetch job to craw from redis"""

    def __init__(self):
        db = rd
        pool = redis.ConnectionPool(host=rd['host'], port=rd['port'], decode_responses=True)
        self.con = redis.Redis(connection_pool=pool)

    @property
    def redis(self):
        return self.con

