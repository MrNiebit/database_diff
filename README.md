# 需求背景

在现场升级是个问题，尤其当我们跨版本升级，中间可能有很多数据库变更，我们升级的时候可能会遇到各种问题；

具体问题如下：
- 数据库变更（基本需求已完成）
- Nacos配置变更

为了解决跨版本升级DB变更的问题开发此脚本

还在开发中...

# 流程

由于线上环境和开发环境不同，所以可以采用将开发环境的数据库表结构导出

解析成 固定结构格式的文件
```json
[{
  "name": "xxx",
  "col_info_list": {},
  "ddl": ""
}]
```

导出线上环境固定结构格式的文件
然后做对比，并生成需要执行的DB语句；

# 使用教程

1、安装依赖库
```bash
pip install -r requirement.txt
```

2、导出文件
  python diff_base.py -env dev
    --env 使用的环境prod or dev
    
3、数据库比较的两种形式
  python diff_base.py --type file 以文件的形式比较
  python diff_base.py --type source 使用数据源的形式进行比较

> 使用文件的形式比较前，请先导出文件

当我们打包成对应平台（window、linux...）时，执行命令也会有所改变
Linux: 给了文件的执行权限，可以使用 ./diff_base --t ...
window: 使用 diff_base.exe --t ...
mac: 同Linux

# 打包教程

安装打包库
```bash
pip install pyinstaller
```

打包命令
```bash
pyinstaller -F ./diff_base.py
```

window、Linux、mac 均可以打包，打包之后就不再依赖Python环境

# 目标

- [x] 实现表的新增语句生成
- [x] 实现字段的增改的语句生成
- [ ] 实现索引的语句的生成
- [ ] 支持多种类型数据库
- [x] 通过直接连接的方式进行对比

