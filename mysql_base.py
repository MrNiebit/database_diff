#!/usr/bin/python3
# --*-- coding: utf-8 --*--
# @Author: gitsilence
# @Time: 2021/9/4 11:14 下午
import pymysql
import configparser


def get_config(env):
    cfg = configparser.ConfigParser()
    cfg.read('./config/config.ini', encoding='UTF-8')
    host = cfg.get(env, 'host')
    port = cfg.get(env, 'port')
    username = cfg.get(env, 'username')
    password = cfg.get(env, 'password')
    database = cfg.get(env, 'database')
    charset = cfg.get(env, 'charset')
    return host, port, username, password, database, charset


class MySQLConnect(object):

    def __init__(self, env='DEV'):
        env = env.lower()
        print('当前环境：', env)
        self.env = env
        host, port, username, password, database, charset = get_config(env)
        self.connection = pymysql.connect(
            host=host,
            port=int(port),
            user=username,
            password=password,
            database=database,
            charset=charset
        )

    def get_connection(self):
        """
        获取一个连接对象
        :return:
        """
        return self.connection

    def get_cursor(self):
        """
        获取游标对象
        :return:
        """
        return self.connection.cursor()

    def close_conn(self):
        """
        关闭连接
        :return:
        """
        self.connection.close()
