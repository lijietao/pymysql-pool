#-*- coding: utf-8 -*-

__author__ = 'F4367281'
__version__ = '1.0'
'''
*********************History**************************
create: 2020/5/20
file name:pymysql_pool.py


******************************************************
'''

import time
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
    _INSTANCE = None
    _LIFE_CYCLE = 30*60  # 达到最大空闲数时，连接存活最大时间(秒),
    _INSTANCE_META = dict()

    def __init__(self, host: str, port: int, user: str, password: str, dbname: str, max_conn: int, max_idle_conn: int):
        self._max_conn = max_conn
        self._max_idle_conn = max_idle_conn
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._dbname = dbname

    def __new__(cls, *args, **kwargs):
        if cls._INSTANCE is None:
            cls._INSTANCE = object.__new__(cls)

        return cls._INSTANCE

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
        conn = Connection(host=self._host,
                          port=self._port,
                          user=self._user,
                          password=self._password,
                          db=self._dbname,
                          cursorclass=DictCursor
                          )

        PyMysqlPool._INSTANCE_META.update({conn: time.time()})
        return conn

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
                PyMysqlPool._RESOURCE = 1

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

        try:
            if conn and isinstance(conn, Connection):
                _instance_create_time = PyMysqlPool._INSTANCE_META.get(conn)
                # 当前空闲连数接小于定义的最大空闲连接数,且实例创建时间与当前时间的差值小于定义的_LIFE_CYCLE,则该连接实例回收;反之不回收
                if PyMysqlPool._IDLE_CONN.__len__() < self._max_idle_conn and (time.time() - _instance_create_time) < PyMysqlPool._LIFE_CYCLE:
                    PyMysqlPool._IDLE_CONN.append(conn)
                    PyMysqlPool._USE_CONN.remove(conn)

                else:
                    # 从连接实例原始数据里面移除该连接实例
                    PyMysqlPool._INSTANCE_META.pop(conn)

        except Exception as err:
            log.error("%s, %s", err, traceback.format_exc())


