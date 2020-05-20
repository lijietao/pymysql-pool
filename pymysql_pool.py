#-*- coding: utf-8 -*-

__author__ = 'lijietao'
__version__ = '1.0'
'''
*********************History**************************
create: 2020/5/20
file name:pymysql_pool.py


******************************************************
'''

import traceback
import threading
import logging
from pymysql.cursors import DictCursor
from pymysql.connections import Connection


log = logging.getLogger(__file__)
log.setLevel(logging.DEBUG)


class PyMysqlPool(object):

    _LOCK = threading.Lock()
    _IDLE_CONN = list()
    _USE_CONN = list()
    _RESOURCE = None

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, max_conn: int, max_idle_conn: int):
        self._max_conn = max_conn
        self._max_idle_conn = max_idle_conn
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._dbname = dbname

    def _create_resource(self):
        """
        创建资源
        :return:
        """
        while PyMysqlPool._IDLE_CONN.__len__() < self._max_idle_conn:
            try:

                PyMysqlPool._IDLE_CONN.append(self._create_connect())

            except Exception as err:
                log.error("%s, %s", err, traceback.format_exc())

        log.info("mysql resource pool create success, pool size: %s", PyMysqlPool._IDLE_CONN.__len__())

    def _create_connect(self):
        """
        创建并返回连接实例
        :return:
        """
        return Connection(host=self._host,
                          port=self._port,
                          user=self._user,
                          password=self._password,
                          db=self._dbname,
                          cursorclass=DictCursor
                          )

    def get_connection(self):
        """
        获取连接资源
        :return:
        """
        PyMysqlPool._LOCK.acquire()
        conn = None
        try:

            if PyMysqlPool._IDLE_CONN:
                conn = PyMysqlPool._IDLE_CONN.pop(0)
                PyMysqlPool._USE_CONN.append(conn)

            else:
                if (PyMysqlPool._IDLE_CONN.__len__() + PyMysqlPool._USE_CONN.__len__()) < self._max_conn:
                    conn = self._create_connect()
                    PyMysqlPool._USE_CONN.append(conn)

            conn.ping()

        except Exception as err:
            if conn in PyMysqlPool._USE_CONN:
                PyMysqlPool._USE_CONN.remove(conn)

            log.error("%s, %s", err, traceback.format_exc())

        finally:
            PyMysqlPool._LOCK.release()
            if conn is None:
                raise Exception("can not get connection from resource pool")

            return conn

    def _init_resource(self):
        """
        初始化资源
        :return:
        """
        if PyMysqlPool._RESOURCE is None:
            PyMysqlPool._LOCK.acquire()
            if PyMysqlPool._RESOURCE is None:
                self._create_resource()

                PyMysqlPool._LOCK.release()

    def get_mysql_connection(self):
        """
        对接现有的工具类
        :return:
        """
        self._init_resource()
        return self.get_connection()

    def release_connection(self, conn: Connection):
        """
        释放连接资源,把连接放到资源池里面
        :param conn:
        :return:
        """

        if conn and isinstance(conn, Connection):
            PyMysqlPool._IDLE_CONN.append(conn)
            PyMysqlPool._USE_CONN.remove(conn)
            


