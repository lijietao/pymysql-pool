#-*- coding: utf-8 -*-

__author__ = 'lijietao'
__version__ = '1.0'
'''
*********************History**************************
create: 2020/5/20
file name:test1.py


******************************************************
'''

import traceback
import time
import threading
from mysql_utility import MysqlUtility


def test1():

    a = 0
    s_time = time.time()
    while a < 500:
        a += 1
        try:
            mysql = MysqlUtility(host="127.0.0.1",
                                 port=3306,
                                 user="xxx",
                                 password="xxx",
                                 dbname="test",
                                 max_conn=50,
                                 max_idle_conn=20
                                 )

            sql = "SELECT * FROM `test` "
            s = time.time()
            res = mysql.findAll(sql)
            mysql.close()
            print(threading.get_ident(), time.time() - s, a, res)

        except Exception as err:
            print("test error", err, traceback.format_exc())

        # time.sleep(0.1)

    print(threading.get_ident(), " use time, ", time.time() - s_time)


if __name__ == "__main__":
    s_time = time.time()
    for i in range(100):
        t = threading.Thread(target=test1, args=()).start()




