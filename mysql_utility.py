#-*- coding: utf-8 -*-

__author__ = 'F4367281'
__version__ = '1.0'
'''
*********************History**************************
create: 2020/5/20
file name:mysql_utility.py


******************************************************
'''

from pymysql_pool import PyMysqlPool


class MysqlUtility(object):

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, max_conn: int, max_idle_conn: int, transaction: bool=False):
        self._mysql_pool = PyMysqlPool(host=host,
                                       port=port,
                                       user=user,
                                       password=password,
                                       dbname=dbname,
                                       max_conn=max_conn,
                                       max_idle_conn=max_idle_conn
                                       )

        self._conn = None
        self._transaction = transaction
        self._conn = self._mysql_pool.get_mysql_connection()
        # 是否开启事务
        if self._transaction:
            self._conn.begin()

    def findAll(self, sql, param=None):
        """
        查询所有的数据
        :param sql:
        :param param:
        :return:
        """
        cursor = self._conn.cursor()
        if param is None:
            cursor.execute(sql)

        else:
            cursor.execute(sql, param)

        result = cursor.fetchall()
        cursor.close()
        return result

    def _query(self, sql, params=None):
        """
        执行查询sql,并返回查询结果
        :param sql:
        :return:
        """
        cursor = self._conn.cursor()
        if params is None:
            count = cursor.execute(sql)

        else:
            count = cursor.execute(sql, params)

        cursor.close()
        return count

    def update(self, sql, params=None):
        """
        更新操作
        :param sql:
        :param params:
        :return:
        """
        return self._query(sql, params)

    def delete(self, sql, params=None):
        """
        删除操作
        :param sql:
        :param params:
        :return:
        """
        self._query(sql, params)
        return True

    def insertOne(self, sql, value=None):
        """
        插入一条数据
        :param sql:
        :param params:
        :return:
        """
        cursor = self._conn.cursor()
        if value is None:
            cursor.execute(sql)

        else:
            cursor.execute(sql, value)

        cursor.close()
        return True

    def insertMany(self, sql, values):
        """
        批量插入数据
        :param sql:
        :param values:
        :return:
        """
        cursor = self._conn.cursor()
        cursor.executemany(sql, values)
        cursor.close()
        return True

    def batch_exec(self, sqls):
        """
        批量执行sql
        :param sqls:
        :return:
        """
        cursor = self._conn.cursor()
        for sql in sqls:
            cursor.execute(sql)

        cursor.close()

    def commit(self, close=True):
        """
        提交操作
        :return:
        """
        self._conn.commit()
        if close:
            self.close()

    def rollback(self, close=True):
        """
        回滚操作
        :return:
        """
        self._conn.rollback()
        if close:
            self.close()

    def close(self):
        if self._conn is not None:
            self._conn.rollback()
            self._mysql_pool.release_connection(self._conn)



