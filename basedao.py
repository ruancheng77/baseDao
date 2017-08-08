import json, os, sys, time, pymysql, pprint

from DBUtils import PooledDB

def print(*args):
    pprint.pprint(args)

def get_time():
    '获取时间'
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def stitch_sequence(seq=None, suf=None):
    '如果参数（"suf"）不为空，则根据特殊的suf拼接列表元素，返回一个字符串'
    if seq is None: raise Exception("Parameter seq is None");
    if suf is None: suf = ","
    r = str()
    for s in seq:
        r += s + suf
    return r[:-len(suf)]

class BaseDao(object):
    """
    简便的数据库操作基类
    """
    def __init__(self, creator=pymysql, host="localhost",port=3306, user=None, password="",
                    database=None, charset="utf8"):
        if host is None: raise Exception("Parameter [host] is None.")
        if port is None: raise Exception("Parameter [port] is None.")
        if user is None: raise Exception("Parameter [user] is None.")
        if password is None: raise Exception("Parameter [password] is None.")
        if database is None: raise Exception("Parameter [database] is None.")
        # 数据库连接配置
        self.__config = dict({
            "creator" : creator, "charset":charset, "host":host, "port":port, 
            "user":user, "password":password, "database":database
        })
        self.__database = self.__config["database"]     # 用于存储查询数据库
        self.__tableName = None                         # 用于临时存储当前查询表名
        # 初始化
        self.__init_connect()                           # 初始化连接
        self.__init_params()                            # 初始化参数
        print(get_time(), self.__database, "数据库初始化成功。")
        
    def __del__(self):
        '重写类被清除时调用的方法'
        if self.__cursor: self.__cursor.close()
        if self.__conn: self.__conn.close()
        print(get_time(), self.__database, "连接关闭")

    def __init_connect(self):
        self.__conn = PooledDB.connect(**self.__config)
        self.__cursor = self.__conn.cursor()

    def __init_params(self):
        '初始化参数'
        self.__init_table_dict()
        self.__init__table_column_dict_list()

    def __init__information_schema_columns(self):
        "查询 information_schema.`COLUMNS` 中的列"
        sql =   """ SELECT COLUMN_NAME FROM information_schema.`COLUMNS`
                    WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS'
                """
        result_tuple = self.__exec_query(sql)
        column_list = [r[0] for r in result_tuple]
        return column_list

    def __init_table_dict(self):
        "查询配置数据库中改的所有表"
        schema_column_list = self.__init__information_schema_columns()
        stitch_str = stitch_sequence(schema_column_list)
        sql1 =  """ SELECT TABLE_NAME FROM information_schema.`TABLES`
                    WHERE TABLE_SCHEMA='%s'
                """ %(self.__database)
        table_tuple = self.__exec_query(sql1)
        self.__table_dict = {t[0]:{} for t in table_tuple}
        for table in self.__table_dict.keys():
            sql =   """ SELECT %s FROM information_schema.`COLUMNS`
                        WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
                    """ %(stitch_str, self.__database, table)
            column_tuple = self.__exec_query(sql)
            column_dict = {}
            for vs in column_tuple:
                d = {k:v for k,v in zip(schema_column_list, vs)}
                column_dict[d["COLUMN_NAME"]] = d
            self.__table_dict[table] = column_dict

    def __init__table_column_dict_list(self):
        self.__table_column_dict_list = {}
        for table, column_dict in self.__table_dict.items():
            column_list = [column for column in column_dict.keys()]
            self.__table_column_dict_list[table] = column_list
        
    def __exec_query(self, sql, single=False):
        '''
        执行查询方法
        - @sql    查询 sql
        - @single 是否查询单个结果集，默认False
        '''
        try:
            self.__cursor.execute(sql)
            print(get_time(), "SQL[%s]"%sql)
            if single:
                result_tuple = self.__cursor.fetchone()
            else:
                result_tuple = self.__cursor.fetchall()
            return result_tuple
        except Exception as e:
            print(e)

    def __exec_update(self, sql):
        try:
            # 获取数据库游标
            result = self.__cursor.execute(sql)
            print(get_time(), "SQL[%s]"%sql)
            self.__conn.commit()
            return result
        except Exception as e:
            print(e)
            self.__conn.rollback()

    def __parse_result(self, result):
        '用于解析单个查询结果，返回字典对象'
        if result is None: return None
        obj = {k:v for k,v in zip(self.__column_list, result)}
        return obj

    def __parse_results(self, results):
        '用于解析多个查询结果，返回字典列表对象'
        if results is None: return None
        objs = [self.__parse_result(result) for result in results]
        return objs

    def __getpk(self, tableName):
        if self.__table_dict.get(tableName) is None: raise Exception(tableName, "is not exist.")
        for column, column_dict in self.__table_dict[tableName].items():
            if column_dict["COLUMN_KEY"] == "PRI": return column

    def __get_table_column_list(self, tableName=None):
        '查询表的字段列表, 将查询出来的字段列表存入 __fields 中'
        return self.__table_column_dict_list[tableName]

    def __query_util(self, filters=None):
        """
        SQL 语句拼接方法
        @filters 过滤条件
        """
        sql = r'SELECT #{FIELDS} FROM #{TABLE_NAME} WHERE 1=1 #{FILTERS}'
        # 拼接查询表
        sql = sql.replace("#{TABLE_NAME}", self.__tableName)
        # 拼接查询字段
        FIELDS = stitch_sequence(self.__get_table_column_list(self.__tableName))
        sql = sql.replace("#{FIELDS}", FIELDS)
        # 拼接查询条件（待优化）
        if filters is None:
            sql = sql.replace("#{FILTERS}", "")
        else:
            FILTERS =  ""
            if not isinstance(filters, dict):
                raise Exception("Parameter [filters] must be dict type. ")
            isPage = False
            if filters.get("_limit_"): isPage = True
            if isPage: beginindex, limit = filters.pop("_limit_")
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
                else:                # 拼接等于
                    FILTERS += "AND %s='%s' "%(k, v)
            sql = sql.replace("#{FILTERS}", FILTERS)
            if isPage: sql += "LIMIT %d,%d"%(beginindex, limit)
        return sql

    def __check_params(self, tableName):
        '''
        检查参数
        '''
        if tableName is None and self.__tableName is None:
            raise Exception("Parameter [tableName] is None.")
        elif self.__tableName is None or self.__tableName != tableName:
            self.__tableName = tableName
            self.__column_list = self.__table_column_dict_list[self.__tableName]

    def select_one(self, tableName=None, filters={}):
        '''
        查询单个对象
        @tableName 表名
        @filters 过滤条件
        @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_params(tableName)
        sql = self.__query_util(filters)
        result = self.__exec_query(sql, single=True)
        return self.__parse_result(result) 

    def select_pk(self, tableName=None, primaryKey=None):
        '''
        按主键查询
        @tableName 表名
        @primaryKey 主键值
        '''
        self.__check_params(tableName)
        filters = {}
        filters.setdefault(self.__getpk(tableName), primaryKey)
        sql = self.__query_util(filters)
        result = self.__exec_query(sql, single=True)
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
        results = self.__exec_query(sql)
        return self.__parse_results(results)

    def count(self, tableName=None):
        '''
        统计记录数
        '''
        self.__check_params(tableName)
        sql = "SELECT count(*) FROM %s"%(self.__tableName)
        result = self.__exec_query(sql, single=True)
        return result[0]

    def select_page(self, tableName=None, pageNum=1, limit=10, filters={}):
        '''
        分页查询
        @tableName 表名
        @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_params(tableName)
        totalCount = self.count(tableName)
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
        result_tuple = self.__exec_query(sql)
        return self.__parse_results(result_tuple)

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
    user = base.select_one("user")
    print(user)
    ########################################################################
    # users = base.select_all("user")
    # print(users)
    ########################################################################
    filter1 = {
        "status":1,
        "_in_id":"1,2,3,4,5",
        "_like_name":"zhang",
        "_ne_name":"wangwu"
    }
    user_filters = base.select_all("user", filter1)
    print(user_filters)
    ########################################################################
    role = base.select_one("role")
    print(role)
    ########################################################################
    user_pk = base.select_pk("user", 2)
    print(user_pk)
    ########################################################################
    user_limit = base.select_page("user", 1, 10)
    print(user_limit)
    ########################################################################
