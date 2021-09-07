#!/usr/bin/python3
# --*-- coding: utf-8 --*--
# @Author: gitsilence
# @Time: 2021/9/5 5:46 下午
from mysql_export_table_struct import ExportObj
import json
import asyncio
import copy
from pprint import pprint

asyncio.Semaphore(10)
loop = asyncio.get_event_loop()


async def find_data_info_by_name(data_list, name):
    """
    根据名称 查询 list中的指定元素
    :param data_list:
    :param name:
    :return:
    """
    for data in data_list:
        if name == data['name']:
            data_list.remove(data)
            return data
    return None


async def field_compare(dev_data, prod_data):
    dev_tmp = copy.deepcopy(dev_data)
    prod_tmp = copy.deepcopy(prod_data)
    print('field  compare')
    dml_sql_list = ['\n -- 目前生成的dml语句 准确度不高，仅供参考，具体还要看开发环境的数据库；等待后续优化']
    # 多个表
    for dev in dev_tmp:
        table_info = await find_data_info_by_name(prod_tmp, dev['name'])
        if table_info is None:

            continue
        # 找到对应的表信息后，进行字段对比
        for dev_field in dev['col_info_list']:
            prod_field  = await find_data_info_by_name(table_info['col_info_list'], dev_field['name'])
            if prod_field is None:
                # 如果字段不存在，就要 添加了
                dml_sql_list.append(f'ALTER TABLE {dev["name"]} ADD {dev_field["name"]} {dev_field["data_type"]}'
                                    f'{"" if dev_field["length"] is None else "({})".format(dev_field["length"])}'
                                    f' {"NOT NULL" if dev_field["not_null"] else "NULL"} '
                                    f' {"" if dev_field["comment"] is None else "COMMENT {}".format(dev_field["comment"]) + " "} ;')
                # 是否是主键
                if dev_field['PK']:
                    dml_sql_list.append(f'\\n ALTER TABLE {dev["name"]} ADD CONSTRAINT {dev_field["name"]}_pk '
                                        f'PRIMARY KEY ({dev_field["name"]}); \\n')
                continue
            # 进行字段比较吧
            tmp_sql = f'ALTER TABLE {dev["name"]} modify {dev_field["name"] }'
            if dev_field['data_type'] != prod_field['data_type'] or dev_field['length'] != prod_field['length']:
                # 如果不同 以开发环境为准
                tmp_sql += f' {dev_field["data_type"]}{ "" if dev_field["length"] is None else "({})".format(dev_field["length"])}' \
                           f' {"NOT NULL" if dev_field["not_null"] else "NULL"} ' \
                           f'{"" if dev_field["comment"] is None else "COMMENT {}".format(dev_field["comment"]) + " "} ;'
                dml_sql_list.append(tmp_sql)

    with open('./files/prod_need_execute_dml.sql', 'w', encoding='UTF-8') as f:
        f.write('\n\n'.join(dml_sql_list))
    pass


async def table_name_compare(dev_data, prod_data):
    """
    表名进行对比，并生成需要建表的语句
    :param dev_data: 生产环境数据库数据
    :param prod_data: 线上环境数据库数据
    :return:
    """
    dev_tmp = copy.deepcopy(dev_data)
    prod_tmp = copy.deepcopy(prod_data)
    print('table_name compare')
    prod_name_list = [prod['name'] for prod in prod_tmp]
    content_list = []
    for dev in dev_tmp:
        if dev['name'] not in prod_name_list:
            # 线上环境没这个表，生成DDL语句
            content_list.append(dev['ddl'])
    # 写入到文件
    with open('./files/prod_need_execute_ddl.sql', 'w', encoding='UTF-8') as f:
        f.write(';\n\n'.join(content_list) + ';')
    print('ddl语句生成成功')


def get_data_from_file():
    try:
        with open('./files/sql-dev.json', 'r', encoding='UTF-8') as f:
            dev_data = json.load(f)
        with open('./files/sql-prod.json', 'r', encoding='UTF-8') as f:
            prod_data = json.load(f)
    except RuntimeError:
        print('读取文件异常')
    return dev_data, prod_data


if __name__ == '__main__':
    # obj = ExportObj(env='prod')
    # obj.export_to_sql_file()
    # -- 读取文件的形式 --
    # dev_data, prod_data = get_data_from_file()
    # 方法异步

    #  -- 使用数据源的形式 --
    dev_data = ExportObj(env='dev').get_table_info_list()
    prod_data = ExportObj(env='prod').get_table_info_list()

    tasks = [
        asyncio.ensure_future(field_compare(dev_data, prod_data)),
        asyncio.ensure_future(table_name_compare(dev_data, prod_data))
    ]
    loop.run_until_complete(asyncio.wait(tasks))
    loop.close()
    pass
