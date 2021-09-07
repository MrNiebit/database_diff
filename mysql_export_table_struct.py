#!/usr/bin/python3
# --*-- coding: utf-8 --*--
# @Author: gitsilence
# @Time: 2021/9/4 11:13 下午
from mysql_base import MySQLConnect
from ddlparse import DdlParse
import json


class ExportObj(object):

    def __init__(self, env='dev'):
        self.mysql_obj = MySQLConnect(env=env)
        self.cursor = self.mysql_obj.get_cursor()
        pass

    def __get_table_struct(self, table_name_list):
        """
        根据表名 获取表结构
        :param table_name_list:
        :return: ('表名', '表的ddl语句')
        """
        table_struct_list = []
        for table_name in table_name_list:
            self.cursor.execute(f'show create table {table_name}')
            tab_data = self.cursor.fetchall()
            # tab_data是一个二维元组，第一个元素是 表结构数据
            # 第一个元素的元组是 存放的 表名 和 表结构，这里只需要结构，所以不取第一个元素
            # print(tab_data)
            table_struct_list.append(tab_data[0])
            # table_struct_list.append(''.join(tab_data[0][1:]))
        return table_struct_list

    def get_all_table_name(self):
        """
        获取所有的表名
        :return:
        """
        all_table = []
        self.cursor.execute('show tables')
        res_data = self.cursor.fetchall()
        for data in res_data:
            # data 类型 是元组
            all_table.append(data[0])
        return all_table

    def __generate_col_info(self, ddl):
        """
        根据ddl语句 生成参数信息
        :param ddl:
        :return:
        """
        field_list = []
        ddl_obj = DdlParse().parse(ddl, source_database=DdlParse.DATABASE.mysql)
        for col in ddl_obj.columns.values():
            col_info = {}
            col_info["name"] = col.name
            col_info["data_type"] = col.data_type
            col_info["length"] = col.length
            col_info["precision(=length)"] = col.precision
            col_info["scale"] = col.scale
            col_info["is_unsigned"] = col.is_unsigned
            col_info["is_zerofill"] = col.is_zerofill
            col_info["constraint"] = col.constraint
            col_info["not_null"] = col.not_null
            col_info["PK"] = col.primary_key
            col_info["unique"] = col.unique
            col_info["auto_increment"] = col.auto_increment
            col_info["distkey"] = col.distkey
            col_info["sortkey"] = col.sortkey
            col_info["encode"] = col.encode
            col_info["default"] = col.default
            col_info["character_set"] = col.character_set
            # col_info["bq_legacy_data_type"]   = col.bigquery_legacy_data_type
            # col_info["bq_standard_data_type"] = col.bigquery_standard_data_type
            col_info["comment"] = col.comment
            col_info["description(=comment)"] = col.description
            # col_info["bigquery_field"]        = json.loads(col.to_bigquery_field())
            field_list.append(col_info)
        return field_list

    def __resolve_table_struct(self, table_struct_list):
        """
        解析 表结构
        :param table_struct_list:
        :return:
        """
        table_list = []
        for struct in table_struct_list:
            table_dic = {}
            table_dic['name'] = struct[0]
            table_dic['ddl'] = struct[1]
            table_dic['col_info_list'] = self.__generate_col_info(struct[1])
            table_list.append(table_dic)
        return table_list

    def __export_file(self, json_data):
        with open(f'./files/sql-{self.mysql_obj.env}.json', 'w', encoding='UTF-8') as f:
            json.dump(json_data, f, ensure_ascii=False)

    def get_table_info_list(self):
        tables = self.get_all_table_name()
        table_struct_list = self.__get_table_struct(tables)
        return self.__resolve_table_struct(table_struct_list)

    def export_to_sql_file(self):
        self.__export_file(self.get_table_info_list())


if __name__ == '__main__':

    pass
