#!/usr/bin/env python3
# -*- coding=utf-8 -*-

'''
基于 DBUtils 和 pymysql 结合的简便操作数据库的类.
'''
__author__ = "阮程"

import logging
import time

import pymysql
from DBUtils import PooledDB

logging.basicConfig(
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    format='%(asctime)s [%(levelname)s] %(message)s'
)

def get_time(fmt=None):
    '''
    获取当前时间
    - @param: fmt 时间格式化字符串
    '''
    fmt = fmt or "%Y-%m-%d %H:%M:%S"
    return time.strftime(fmt, time.localtime())


def stitch_sequence(seq=None, is_field=True, suf=None):
    '''
    序列拼接方法, 用于将序列拼接成字符串
    - :seq: 拼接序列
    - :suf: 拼接后缀(默认使用 ",")
    - :is_field: 是否为数据库字段序列
    '''
    if seq is None:
        raise Exception("Parameter seq is None")
    suf = suf or ","
    res = str()
    for item in seq:
        res += '`%s`%s' % (item, suf) if is_field else '%s%s' % (item, suf)
    return res[:-len(suf)]


class BaseDao(object):
    """
    简便的数据库操作基类，该类所操作的表必须有主键
    初始化参数如下：
    - :creator: 创建连接对象（默认: pymysql）
    - :host: 连接数据库主机地址(默认: localhost)
    - :port: 连接数据库端口(默认: 3306)
    - :user: 连接数据库用户名(默认: None), 如果为空，则会抛异常
    - :password: 连接数据库密码(默认: None), 如果为空，则会抛异常
    - :database: 连接数据库(默认: None), 如果为空，则会抛异常
    - :chatset: 编码(默认: utf8)
    - :table: 初始化 BaseDao 对象的数据库表名(默认: None), 如果为空，
    则会初始化该数据库下所有表的信息, 如果不为空，则只初始化传入的 table 的表
    """

    def __init__(self, creator=pymysql, host="localhost", port=3306, user=None, password=None,
                 database=None, charset="utf8", table=None):
        if host is None:
            raise ValueError("Parameter [host] is None.")
        if port is None:
            raise ValueError("Parameter [port] is None.")
        if user is None:
            raise ValueError("Parameter [user] is None.")
        if password is None:
            raise ValueError("Parameter [password] is None.")
        if database is None:
            raise ValueError("Parameter [database] is None.")
        if table is None:
            print(
                "WARNING >>> Parameter [table] is None. All tables will be initialized.")
        start = time.time()
        # 执行初始化
        self._config = dict({
            "creator": creator, "charset": charset, "host": host, "port": port,
            "user": user, "password": password, "database": database
        })
        self._database = database
        self._table = table
        self._init_connect()
        self._init_params()
        end = time.time()
        logging.info("[{0}] 数据库初始化成功。耗时：{1} ms。".format(database, (end - start)))

    def __del__(self):
        '重写类被清除时调用的方法'
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()
        logging.debug("[{0}] 连接关闭。".format(self._database))

    def _init_connect(self):
        '初始化连接'
        try:
            self.__conn = PooledDB.connect(**self._config)
            self.__cursor = self.__conn.cursor()
        except Exception as e:
            logging.error(e)

    def _init_params(self):
        '初始化参数'
        self._table_dict = {}
        self._information_schema_columns = []
        self._table_column_dict_list = {}
        if self._table is None:
            self._init_table_dict_list()
            self._init_table_column_dict_list()
        else:
            self._init_table_dict(self._table)
            self._init_table_column_dict_list()
            self._column_list = self._table_column_dict_list[self._table]

    def _init_information_schema_columns(self):
        "查询 information_schema.`COLUMNS` 中的列"
        sql = """   SELECT COLUMN_NAME
                    FROM information_schema.`COLUMNS`
                    WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS'
                """
        result_tuple = self.execute_query(sql)
        column_list = [r[0] for r in result_tuple]
        self._information_schema_columns = column_list

    def _init_table_dict(self, table_name):
        '初始化表'
        if not self._information_schema_columns:
            self._init_information_schema_columns()
        stitch_str = stitch_sequence(self._information_schema_columns)
        sql = """   SELECT %s FROM information_schema.`COLUMNS`
                    WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
                """ % (stitch_str, self._database, table_name)
        column_tuple = self.execute_query(sql)
        column_dict = {}
        for column in column_tuple:
            column_dict_item = {key: value for key, value in zip(
                self._information_schema_columns, column)}
            column_dict[column_dict_item["COLUMN_NAME"]] = column_dict_item
        self._table_dict[table_name] = column_dict

    def _init_table_dict_list(self):
        "初始化表字典对象"
        if not self._information_schema_columns:
            self._init_information_schema_columns()
        sql = "SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA='%s'" % (
            self._database)
        table_tuple = self.execute_query(sql)
        self._table_dict = {t[0]: {} for t in table_tuple}
        for table in table_tuple:
            self._init_table_dict(table[0])

    def _init_table_column_dict_list(self):
        '''初始化表字段字典列表'''
        for table, column_dict in self._table_dict.items():
            column_list = [column for column in column_dict.keys()]
            self._table_column_dict_list[table] = column_list

    def _parse_result(self, result):
        '用于解析单个查询结果，返回字典对象'
        if result is None:
            return None
        obj = {key: value for key, value in zip(self._column_list, result)}
        return obj

    def _parse_results(self, results):
        '用于解析多个查询结果，返回字典列表对象'
        if results is None:
            return None
        objs = [self._parse_result(result) for result in results]
        return objs

    def _get_primary_key(self, table_name):
        '获取表对应的主键字段'
        if self._table_dict.get(table_name) is None:
            raise Exception(table_name, "is not exist.")
        for column, column_dict in self._table_dict[table_name].items():
            if column_dict["COLUMN_KEY"] == "PRI":
                return column

    def _get_table_column_list(self, table_name=None):
        '查询表的字段列表, 将查询出来的字段列表存入 __fields 中'
        return self._table_column_dict_list[table_name]

    def _check_table_name(self, table_name):
        '''验证 table_name 参数'''
        if table_name is None:
            if self._table is None:
                raise Exception("Parameter [table_name] is None.")
        else:
            if self._table != table_name:
                self._table = table_name
                self._column_list = self._table_column_dict_list[self._table]

    def execute_query(self, sql=None, single=False):
        '''执行查询 SQL 语句
        - :sql: sql 语句
        - :single: 是否查询单个结果集，默认False
        '''
        try:
            if sql is None:
                raise Exception("Parameter sql is None.")
            logging.info("[%s] SQL >>> [%s]" % (self._database, sql))
            self.__cursor.execute(sql)
            return self.__cursor.fetchone() if single else self.__cursor.fetchall()
        except Exception as e:
            logging.error(e)

    def execute_update(self, sql=None):
        '''执行更新 SQL 语句
        - :sql: sql 语句
        '''
        try:
            if sql is None:
                raise Exception("Parameter sql is None.")
            logging.info("[%s] SQL >>> [%s]" % (self._database, sql))
            result = self.__cursor.execute(sql)
            self.__conn.commit()
            return result
        except Exception as e:
            logging.error(e)
            self.__conn.rollback()

    def select_one(self, table_name=None, filters=None):
        '''查询单个对象
        - @table_name 表名
        - @filters 过滤条件
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self._check_table_name(table_name)
        if filters is None:
            filters = {}
        stitch_str = stitch_sequence(
            self._get_table_column_list(self._table))
        sql = "SELECT %s FROM %s" % (stitch_str, self._table)
        sql = QueryUtil.query_sql(sql, filters)
        result = self.execute_query(sql, True)
        return self._parse_result(result)

    def select_pk(self, table_name=None, primary_key=None):
        '''按主键查询
        - @table_name 表名
        - @primary_key 主键值
        '''
        self._check_table_name(table_name)
        stitch_str = stitch_sequence(
            self._get_table_column_list(self._table))
        sql = "SELECT %s FROM %s" % (stitch_str, self._table)
        sql = QueryUtil.query_sql(sql, {self._get_primary_key(self._table): primary_key})
        result = self.execute_query(sql, True)
        return self._parse_result(result)

    def select_all(self, table_name=None, filters=None):
        '''查询所有
        - @table_name 表名
        - @filters 过滤条件
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self._check_table_name(table_name)
        if filters is None:
            filters = {}
        stitch_str = stitch_sequence(
            self._get_table_column_list(self._table))
        sql = "SELECT %s FROM %s" % (stitch_str, self._table)
        sql = QueryUtil.query_sql(sql, filters)
        results = self.execute_query(sql)
        return self._parse_results(results)

    def count(self, table_name=None):
        '''统计记录数'''
        self._check_table_name(table_name)
        sql = "SELECT count(*) FROM %s" % (self._table)
        result = self.execute_query(sql, True)
        return result[0]

    def select_page(self, table_name=None, page=None, filters=None):
        '''分页查询
        - @table_name 表名
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self._check_table_name(table_name)
        if filters is None:
            filters = {}
        if page is None:
            page = Page()
        filters["page"] = page
        stitch_str = stitch_sequence(
            self._get_table_column_list(self._table))
        sql = "SELECT %s FROM %s" % (stitch_str, self._table)
        sql = QueryUtil.query_sql(sql, filters)
        result_tuple = self.execute_query(sql)
        return self._parse_results(result_tuple)

    def save(self, table_name=None, obj=None):
        '''保存方法
        - @param table_name 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self._check_table_name(table_name)
        if obj is None:
            obj = {}
        primary_key = self._get_primary_key(self._table)
        if primary_key not in obj.keys():
            obj[primary_key] = None
        stitch_str = stitch_sequence(obj.keys())
        value_list = []
        for key, value in obj.items():
            if self._table_dict[self._table][key]["COLUMN_KEY"] != "PKI":
                value = "null" if value is None else '"%s"' % value
            value_list.append(value)
        stitch_value_str = stitch_sequence(value_list, False)
        sql = 'INSERT INTO `%s` (%s) VALUES(%s)' % (
            self._table, stitch_str, stitch_value_str)
        return self.execute_update(sql)

    def update_by_primarykey(self, table_name=None, obj=None):
        '''更新方法(根据主键更新，包含空值)
        - @param table_name 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self._check_table_name(table_name)
        if obj is None:
            obj = {}
        primary_key = self._get_primary_key(self._table)
        if primary_key not in obj.keys() or obj.get(primary_key) is None:
            raise ValueError("Parameter [obj.%s] is None." % primary_key)
        kv_list = []
        where = "WHERE "
        for key, value in obj.items():
            if self._table_dict[table_name][key]["COLUMN_KEY"] != "PRI":
                if value is None:
                    if self._table_dict[table_name][key]["IS_NULLABLE"] == "YES":
                        kv_list.append("%s=null" % (key))
                    else:
                        kv_list.append("%s=''" % (key))
                else:
                    kv_list.append("%s='%s'" % (key, value))
            else:
                where += "%s='%s'" % (key, value)
        sql = "UPDATE `%s` SET %s %s" % (
            self._table, stitch_sequence(kv_list, False), where)
        return self.execute_update(sql)

    def update_by_primarikey_selective(self, table_name=None, obj=None):
        '''更新方法(根据主键更新，不包含空值)
        - @param table_name 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self._check_table_name(table_name)
        if obj is None:
            obj = {}
        primary_key = self._get_primary_key(self._table)
        if primary_key not in obj.keys() or obj.get(primary_key) is None:
            raise ValueError("Parameter [obj.%s] is None." % primary_key)
        where = "WHERE "
        kv_list = []
        for key, value in obj.items():
            if self._table_dict[self._table][key]["COLUMN_KEY"] != "PRI":
                if value is None:
                    continue
                kv_list.append("%s='%s'" % (key, value))
            else:
                where += "%s='%s'" % (key, value)
        sql = "UPDATE `%s` SET %s %s" % (
            self._table, stitch_sequence(kv_list, False), where)
        return self.execute_update(sql)

    def remove_by_primarykey(self, table_name=None, value=None):
        '''删除方法（根据主键删除）
        - @param table_name 表名
        - @param valuej 主键值
        - @return 影响行数
        '''
        self._check_table_name(table_name)
        if value is None:
            raise ValueError("Parameter [value] can not be None.")
        primary_key = self._get_primary_key(self._table)
        sql = "DELETE FROM `%s` WHERE `%s`='%s'" % (
            self._table, primary_key, value)
        return self.execute_update(sql)


class Page(object):
    '分页对象'

    def __init__(self, page_num=1, page_size=10, count=False):
        '''
        Page 初始化方法
        - @param page_num 页码，默认为1
        - @param page_size 页面大小, 默认为10
        - @param count 是否包含 count 查询
        '''
        # 当前页数
        self.page_num = page_num if page_num > 0 else 1
        # 分页大小
        self.page_size = page_size if page_size > 0 else 10
        # 总记录数
        self.total = 0
        # 总页数
        self.pages = 1
        # 起始行（用于 mysql 分页查询）
        self.start_row = (self.page_num - 1) * self.page_size
        # 结束行（用于 mysql 分页查询）
        self.end_row = self.start_row + self.page_size


class QueryUtil(object):
    '''
    SQL 语句拼接工具类：
    - 主方法: querySql(sql, filters)

    参数说明:
    - @param sql：需要拼接的 SQL 语句
    - @param filters：拼接 SQL 的过滤条件

    filters 过滤条件说明：
    - 支持拼接条件如下：
    - 1、等于（如：{"id": 2}, 拼接后为：id=2)
    - 2、不等于（如：{"_ne_id": 2}, 拼接后为：id != 2）
    - 3、小于（如：{"_lt_id": 2}，拼接后为：id < 2）
    - 4、小于等于（如：{"_le_id": 2}，拼接后为：id <= 2）
    - 5、大于（如：{"_gt_id": }，拼接后为：id > 2）
    - 6、大于等于（如：{"_ge_id": }，拼接后为：id >=2）
    - 7、in（如：{"_in_id": "1,2,3"}，拼接后为：id IN(1,2,3)）
    - 8、not in（如：{"_nein_id": "4,5,6"}，拼接后为：id NOT IN(4,5,6)）
    - 9、like（如：{"_like_name": }，拼接后为：name LIKE '%zhang%'）
    - 10、like（如：{"_llike_name": }，拼接后为：name LIKE '%zhang'）
    - 11、like（如：{"_rlike_name": }，拼接后为：name LIKE 'zhang%'）
    - 12、分组（如：{"groupby": "status"}，拼接后为：GROUP BY status）
    - 13、排序（如：{"orderby": "createDate"}，拼接后为：ORDER BY createDate）
    '''
    NE = "_ne_"                 # 拼接不等于
    LT = "_lt_"                 # 拼接小于
    LE = "_le_"                 # 拼接小于等于
    GT = "_gt_"                 # 拼接大于
    GE = "_ge_"                 # 拼接大于等于
    IN = "_in_"                 # 拼接 in
    NE_IN = "_nein_"            # 拼接 not in
    LIKE = "_like_"             # 拼接 like
    LEFT_LIKE = "_llike_"       # 拼接左 like
    RIGHT_LIKE = "_rlike_"      # 拼接右 like
    GROUP = "groupby"           # 拼接分组
    ORDER = "orderby"           # 拼接排序
    ORDER_TYPE = "ordertype"    # 排序类型：asc（升序）、desc（降序）

    @staticmethod
    def __filter_params(filters):
        '''过滤参数条件'''
        res = " WHERE 1=1"
        for key, value in filters.items():
            if key.startswith(QueryUtil.IN):                  # 拼接 in
                res += " AND `%s` IN (" % (key[len(QueryUtil.IN):])
                value_list = value.split(",")
                for value in value_list:
                    res += " %s," % value
                res = res[0:len(res) - 1] + ") "
            elif key.startswith(QueryUtil.NE_IN):               # 拼接 not in
                res += " AND `%s` NOT IN (" % (key[len(QueryUtil.NE_IN):])
                value_list = value.split(",")
                for value in value_list:
                    res += " %s," % value
                res = res[0:len(res) - 1] + ") "
            elif key.startswith(QueryUtil.LIKE):                # 拼接 like
                res += " AND `%s` LIKE '%%%s%%' " % (key[len(QueryUtil.LIKE):], value)
            elif key.startswith(QueryUtil.LEFT_LIKE):           # 拼接左 like
                res += " AND `%s` LIKE '%%%s' " % (
                    key[len(QueryUtil.LEFT_LIKE):], value)
            elif key.startswith(QueryUtil.RIGHT_LIKE):          # 拼接右 like
                res += " AND `%s` LIKE '%s%%' " % (
                    key[len(QueryUtil.RIGHT_LIKE):], value)
            elif key.startswith(QueryUtil.NE):                  # 拼接不等于
                res += " AND `%s` != '%s' " % (key[len(QueryUtil.NE):], value)
            elif key.startswith(QueryUtil.LT):                  # 拼接小于
                res += " AND `%s` < '%s' " % (key[len(QueryUtil.LT):], value)
            elif key.startswith(QueryUtil.LE):                  # 拼接小于等于
                res += " AND `%s` <= '%s' " % (key[len(QueryUtil.LE):], value)
            elif key.startswith(QueryUtil.GT):                  # 拼接大于
                res += " AND `%s` > '%s' " % (key[len(QueryUtil.GT):], value)
            elif key.startswith(QueryUtil.GE):                  # 拼接大于等于
                res += " AND `%s` >= '%s' " % (key[len(QueryUtil.GE):], value)
            else:                                               # 拼接等于
                if isinstance(value, str):
                    res += " AND `%s`='%s' " % (key, value)
                elif isinstance(value, int):
                    res += " AND `%s`=%d " % (key, value)
        return res

    @staticmethod
    def __filter_group(filters):
        '''过滤分组'''
        group = filters.pop(QueryUtil.GROUP)
        res = " GROUP BY %s" % (group)
        return res

    @staticmethod
    def __filter_order(filters):
        '''过滤排序'''
        order = filters.pop(QueryUtil.ORDER)
        order_type = filters.pop(QueryUtil.ORDER_TYPE, "asc")
        res = " ORDER BY `%s` %s" % (order, order_type)
        return res

    @staticmethod
    def __filter_page(filters):
        '''过滤 page 对象'''
        page = filters.pop("page")
        return " LIMIT %d,%d" % (page.start_row, page.end_row)

    @staticmethod
    def query_sql(sql=None, filters=None):
        '''拼接 SQL 查询条件
        - @param sql SQL 语句
        - @param filters 过滤条件
        - @return 返回拼接 SQL
        '''
        if filters is None:
            return sql
        else:
            if not isinstance(filters, dict):
                raise Exception("Parameter [filters] must be dict.")
            group = None
            order = None
            page = None
            if filters.get("groupby") != None:
                group = QueryUtil.__filter_group(filters)
            if filters.get("orderby") != None:
                order = QueryUtil.__filter_order(filters)
            if filters.get("page") != None:
                page = QueryUtil.__filter_page(filters)
            sql += QueryUtil.__filter_params(filters)
            if group:
                sql += group
            if order:
                sql += order
            if page:
                sql += page
        return sql


def _test1():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test",
        "table": "province"
    }
    # 指定初始化 table
    test_dao = BaseDao(**CONFIG)

    # 查询单条记录
    # one = test_dao.select_one()
    # print(one)

    # 查询所有记录
    # all = test_dao.select_all()
    # print(all)

    # 查询分页记录
    # page = test_dao.select_page()
    # print(page)

    # 按主键查询
    one_pk = test_dao.select_pk(primary_key=1)
    print(one_pk)

def _test2():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test"
    }
    # 初始化所有 table
    test_dao = BaseDao(**CONFIG)

    # one1 = test_dao.select_one("province")
    # print(one1)
    # one2 = test_dao.select_one("city")
    # print(one2)

    # filters1 = {
    #     QueryUtil.GE + "id": 5,
    #     QueryUtil.LT + "id": 30,
    #     QueryUtil.ORDER: "id",
    #     QueryUtil.ORDER_TYPE: "desc"
    # }
    # all_filters = test_dao.select_all("province", filters1)
    # print(all_filters)

    filters2 = {
        QueryUtil.LEFT_LIKE + "province": "省",
    }
    page = Page(1, 20)
    page_filters = test_dao.select_page("province", page, filters2)
    print(page_filters)

def _test3():
    CONFIG = {
        "user": "root",
        "password": "root",
        "database": "test",
        "table": "province"
    }
    # 初始化所有 table
    test_dao = BaseDao(**CONFIG)
    province = {
        "id": None,
        "province_id": "830000",
        "province": "测试"
    }

    # test_dao.save(obj=province)
    
    # f1 = {
    #     "province": "测试"
    # }
    # item = test_dao.select_one(filters=f1)
    # print(item)
    # item["province"] = "测试1"
    # test_dao.update_by_primarikey_selective(obj=item)

    # f2 = {
    #     "province": "测试1"
    # }
    # item = test_dao.select_one(filters=f2)
    # test_dao.remove_by_primarykey(value=item["id"])


if __name__ == '__main__':
    # _test1()
    # _test2()
    _test3()
