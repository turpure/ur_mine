#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-01-02 11:06
# Author: turpure


from common.tools import MsSQL

connection = MsSQL()


def get_color_dict():
    """
    key is color_code and value is color_name
    :return:
    """
    color_dict = dict()
    sql = "select color_code,color from joom_color where color_code is not Null"
    with connection as con:
        cur = con.cursor()
        cur.execute(sql)
        ret = cur.fetchall()
        for row in ret:
            color_dict[row[0]] = row[1]
    return color_dict
