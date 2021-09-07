#!/usr/bin/python3
# --*-- coding: utf-8 --*--
# @Author: gitsilence
# @Time: 2021/9/5 5:46 下午
from mysql_export_table_struct import ExportObj
import json
import asyncio
import copy
import sys
import getopt

loop = asyncio.get_event_loop()
script_base = "diff_base"


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
    print('dml语句生成成功')
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


def get_value(args, key1, key2):
    try:
        return args[key1]
    except:
        return args[key2]


def main(argv):
    help_str = '''
    1、导出文件
      {} -env dev
        --env 使用的环境prod or dev
        
    2、数据库比较的两种形式
      {} --type file 以文件的形式比较
      {} --type source 使用数据源的形式进行比较
    '''.format(script_base, script_base, script_base)
    try:
        opts, args = getopt.getopt(argv, "he:t", ['help', 'env=', 'type='])
    except getopt.GetoptError as e:
        print("""
    [Fatal] {}
    Try '{} --help' for more options.
        """.format(e, script_base))
        sys.exit()
    args = {k: v for k, v in opts}
    print(args)
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            print(help_str)
            sys.exit()
        elif opt in ('-e', '--env'):
            # 导出文件
            env = get_value(args, '--env', '-e')
            ExportObj(env=env).export_to_sql_file()
            print('{} 环境数据导出成功'.format(env))
            sys.exit()
        elif opt in ('-t', '--type'):
            compare_type = get_value(args, '-t', '--type')
            dev_data = None
            prod_data = None
            if compare_type == 'file':
                # 导出文件
                dev_data, prod_data = get_data_from_file()
            elif compare_type == 'source':
                dev_data = ExportObj(env='dev').get_table_info_list()
                prod_data = ExportObj(env='prod').get_table_info_list()
            if dev_data is None or prod_data is None:
                print('dev数据 或者 prod数据 不能为空')
                sys.exit()
            tasks = [
                asyncio.ensure_future(field_compare(dev_data, prod_data)),
                asyncio.ensure_future(table_name_compare(dev_data, prod_data))
            ]
            loop.run_until_complete(asyncio.wait(tasks))
            loop.close()


if __name__ == '__main__':
    # obj = ExportObj(env='prod')
    # obj.export_to_sql_file()
    # -- 读取文件的形式 --
    # dev_data, prod_data = get_data_from_file()
    # 方法异步

    # 获取命令行参数
    try:
        main(sys.argv[1:])
    except Exception as e:
        print("""
[Fatal] {}
Try '{} --help' for more options.
        """.format(e, script_base))
        sys.exit()
    pass
