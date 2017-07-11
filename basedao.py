import json, os, sys, time

import pymysql
from DBUtils import PooledDB

class BaseDao(object):
    """
    简便的数据库操作基类
    """
    # 类变量定义在这的时候会出现问题：当程序运行并且实例化了两个不同的（连接数据库不同） BaseDao 时，
    # self.__primaryKey_dict 会出现异常（两个实例的self.__primaryKey_dict相同）暂不知为何引起这个错误。
    # 我们将在 __init__ 方法中定义类成员。
    # __config = {}                   # 数据库连接配置
    # __conn = None                   # 数据库连接
    # __cursor = None                 # 数据库游标
    # __database = None               # 用于临时存储查询数据库
    # __tableName = None              # 用于临时存储查询表名
    # __fields = []                   # 用于临时存储查询表的字段列表
    # __primaryKey_dict = {}          # 用于存储配置中的数据库中所有表的主键

    def __init__(self, creator=pymysql, host="localhost", user=None, password="", database=None, port=3306, charset="utf8"):
        if host is None:
            raise Exception("Parameter [host] is None.")
        if user is None:
            raise Exception("Parameter [user] is None.")
        if password is None:
            raise Exception("Parameter [password] is None.")
        if database is None:
            raise Exception("Parameter [database] is None.")
        if port is None:
            raise Exception("Parameter [port] is None.")
        self.__tableName = None              # 用于临时存储查询表名
        self.__fields = []                   # 用于临时存储查询表的字段列表
        self.__primaryKey_dict = {}          # 用于存储配置中的数据库中所有表的主键
        # 数据库连接配置
        self.__config = dict({
            "creator" : creator, "charset":charset,
            "host":host, "port":port, 
            "user":user, "password":password, "database":database
        })
        # 数据库连接
        self.__conn = PooledDB.connect(**self.__config)
        # 数据库游标
        self.__cursor = self.__conn.cursor()
        # 用于存储查询数据库
        self.__database = self.__config["database"]
        self.__init_primaryKey()
        print(get_time(), self.__database, "数据库连接初始化成功。")
        
    def __del__(self):
        '重写类被清除时调用的方法'
        if self.__cursor:
            self.__cursor.close()
        if self.__conn:
            self.__conn.close()
            print(get_time(), self.__database, "连接关闭")

    def select_one(self, tableName=None, filters={}):
        '''
        查询单个对象
        @tableName 表名
        @filters 过滤条件
        @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_params(tableName)
        sql = self.__query_util(filters)
        self.__cursor.execute(sql)
        result = self.__cursor.fetchone()
        return self.__parse_result(result)

    def select_pk(self, tableName=None, primaryKey=None):
        '''
        按主键查询
        @tableName 表名
        @primaryKey 主键值
        '''
        self.__check_params(tableName)
        filters = {}
        filters.setdefault(str(self.__primaryKey_dict[tableName]), primaryKey)
        sql = self.__query_util(filters)
        self.__cursor.execute(sql)
        result = self.__cursor.fetchone()
        return self.__parse_result(result)
        
    def select_all(self, tableName=None, filters={}):
        '''
        查询所有
        @tableName 表名
        @filters 过滤条件
        @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_params(tableName)
        sql = self.__query_util(filters)
        self.__cursor.execute(sql)
        results = self.__cursor.fetchall()
        return self.__parse_results(results)

    def count(self, tableName=None):
        '''
        统计记录数
        '''
        self.__check_params(tableName)
        sql = "SELECT count(*) FROM %s"%(self.__tableName)
        self.__cursor.execute(sql)
        result = self.__cursor.fetchone()
        return result[0]

    def select_page(self, tableName=None, pageNum=1, limit=10, filters={}):
        '''
        分页查询
        @tableName 表名
        @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_params(tableName)
        totalCount = self.count()
        if totalCount / limit == 0 :
            totalPage = totalCount / limit
        else:
            totalPage = totalCount // limit + 1
        if pageNum > totalPage:
            print("最大页数为%d"%totalPage)
            pageNum = totalPage
        elif pageNum < 1:
            print("页数不能小于1")
            pageNum = 1
        beginindex = (pageNum-1) * limit
        filters.setdefault("_limit_", (beginindex, limit))
        sql = self.__query_util(filters)
        self.__cursor.execute(sql)
        results = self.__cursor.fetchall()
        return self.__parse_results(results)

    def select_database_struts(self):
        '''
        查找当前连接配置中的数据库结构以字典集合
        '''
        sql = '''SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLUMN_KEY, COLUMN_COMMENT
                FROM information_schema.`COLUMNS` 
                WHERE TABLE_SCHEMA="%s" AND TABLE_NAME="{0}" '''%(self.__database)
        struts = {}
        for k in self.__primaryKey_dict.keys():
            self.__cursor.execute(sql.format(k))
            results = self.__cursor.fetchall()
            struts[k] = {}
            for result in results:
                struts[k][result[0]] = {}
                struts[k][result[0]]["COLUMN_NAME"] = result[0]
                struts[k][result[0]]["IS_NULLABLE"] = result[1]
                struts[k][result[0]]["COLUMN_TYPE"] = result[2]
                struts[k][result[0]]["COLUMN_KEY"] = result[3]
                struts[k][result[0]]["COLUMN_COMMENT"] = result[4]
        return self.__config, struts

    def __parse_result(self, result):
        '用于解析单个查询结果，返回字典对象'
        obj = {}
        for k,v in zip(self.__fields, result):
            obj[k] = v
        return obj

    def __parse_results(self, results):
        '用于解析多个查询结果，返回字典列表对象'
        objs = []
        for result in results:
            obj = self.__parse_result(result)
            objs.append(obj)
        return objs

    def __init_primaryKey(self):
        '根据配置中的数据库读取该数据库中所有表的主键集合'
        sql = """SELECT TABLE_NAME, COLUMN_NAME
                FROM  Information_schema.columns
                WHERE COLUMN_KEY='PRI' AND TABLE_SCHEMA='%s'"""%(self.__database)
        self.__cursor.execute(sql)
        results = self.__cursor.fetchall()
        for result in results:
            self.__primaryKey_dict[result[0]] = result[1]

    def __query_fields(self, tableName=None, database=None):
        '查询表的字段列表, 将查询出来的字段列表存入 __fields 中'
        sql = """SELECT column_name
                FROM  Information_schema.columns
                WHERE table_Name = '%s' AND TABLE_SCHEMA='%s'"""%(tableName, database)
        self.__cursor.execute(sql)
        fields_tuple = self.__cursor.fetchall()
        self.__fields = [fields[0] for fields in fields_tuple]

    def __query_util(self, filters=None):
        """
        SQL 语句拼接方法
        @filters 过滤条件
        """
        sql = r'SELECT #{FIELDS} FROM #{TABLE_NAME} WHERE 1=1 #{FILTERS}'
        # 拼接查询表
        sql = sql.replace("#{TABLE_NAME}", self.__tableName)
        # 拼接查询字段
        self.__query_fields(self.__tableName, self.__database)
        FIELDS = ""
        for field in self.__fields:
            FIELDS += field + ", "
        FIELDS = FIELDS[0: len(FIELDS)-2]
        sql = sql.replace("#{FIELDS}", FIELDS)
        # 拼接查询条件（待优化）
        if filters is None:
            sql = sql.replace("#{FILTERS}", "")
        else:
            FILTERS =  ""
            if not isinstance(filters, dict):
                raise Exception("Parameter [filters] must be dict type. ")
            isPage = False
            if filters.get("_limit_"):
                isPage = True
                beginindex, limit = filters.get("_limit_")
            for k, v in filters.items():
                if k.startswith("_in_"):                # 拼接 in
                    FILTERS += "AND %s IN (" %(k[4:])
                    values = v.split(",")
                    for value in values:
                        FILTERS += "%s,"%value
                    FILTERS = FILTERS[0:len(FILTERS)-1] + ") "
                elif k.startswith("_nein_"):            # 拼接 not in
                    FILTERS += "AND %s NOT IN (" %(k[4:])
                    values = v.split(",")
                    for value in values:
                        FILTERS += "%s,"%value
                    FILTERS = FILTERS[0:len(FILTERS)-1] + ") "
                elif k.startswith("_like_"):            # 拼接 like
                    FILTERS += "AND %s like '%%%s%%' " %(k[6:], v)
                elif k.startswith("_ne_"):              # 拼接不等于
                    FILTERS += "AND %s != '%s' " %(k[4:], v)
                elif k.startswith("_lt_"):              # 拼接小于
                    FILTERS += "AND %s < '%s' " %(k[4:], v)
                elif k.startswith("_le_"):              # 拼接小于等于
                    FILTERS += "AND %s <= '%s' " %(k[4:], v)
                elif k.startswith("_gt_"):              # 拼接大于
                    FILTERS += "AND %s > '%s' " %(k[4:], v)
                elif k.startswith("_ge_"):              # 拼接大于等于
                    FILTERS += "AND %s >= '%s' " %(k[4:], v)
                elif k in self.__fields:                # 拼接等于
                    FILTERS += "AND %s = '%s' "%(k, v)
            sql = sql.replace("#{FILTERS}", FILTERS)
            if isPage:
                sql += "LIMIT %d,%d"%(beginindex, limit)

        print(get_time(), sql)
        return sql

    def __check_params(self, tableName):
        '''
        检查参数
        '''
        if tableName:
            self.__tableName = tableName
        else:
            if self.__tableName is None:
                raise Exception("Parameter [tableName] is None.")

def get_time():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

if __name__ == "__main__":
    config = {
        # "creator": pymysql,
        # "host" : "127.0.0.1", 
        "user" : "root", 
        "password" : "root",
        "database" : "test", 
        # "port" : 3306,
        # "charset" : 'utf8'
    }
    base = BaseDao(**config)
    ########################################################################
    # user = base.select_one("user")
    # print(user)
    ########################################################################
    # users = base.select_all("user")
    # print(users)
    ########################################################################
    # filter1 = {
    #     "sex":0,
    #     "_in_id":"1,2,3,4,5",
    #     "_like_name":"zhang",
    #     "_ne_name":"wangwu"
    # }
    # user_filters = base.select_all(tableName="user", filters=filter1)
    # print(user_filters)
    ########################################################################
    # menu = base.select_one(tableName="menu")
    # print(menu)
    ########################################################################
    # user_pk = base.select_pk("user", 2)
    # print(user_pk)
    ########################################################################
    # filter2 = {
    #     "_in_id":"1,2,3,4",
    #     "_like_name":"test"
    # }
    # user_limit = base.select_page("user", 2, 10, filter2)  #未实现
    # print(user_limit)
    ########################################################################
