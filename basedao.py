import json, os, sys, time, pymysql, pprint, logging

logging.basicConfig(
    level=logging.DEBUG, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%a, %d %b %Y %H:%M:%S')

from DBUtils import PooledDB

def print(*args):
    pprint.pprint(args)

def get_time():
    '获取时间'
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

def stitch_sequence(seq=None, suf=None):
    '如果参数（"suf"）不为空，则根据特殊的suf拼接列表元素，返回一个字符串。默认使用 ","。'
    if seq is None: raise Exception("Parameter seq is None");
    if suf is None: suf = ","
    r = str()
    for s in seq:
        r += s + suf
    return r[:-len(suf)]

class BaseDao(object):
    """
    简便的数据库操作基类，该类所操作的表必须有主键
    初始化参数如下：
    - creator: 创建连接对象（默认: pymysql）
    - host: 连接数据库主机地址(默认: localhost)
    - port: 连接数据库端口(默认: 3306)
    - user: 连接数据库用户名(默认: None), 如果为空，则会抛异常
    - password: 连接数据库密码(默认: None), 如果为空，则会抛异常
    - database: 连接数据库(默认: None), 如果为空，则会抛异常
    - chatset: 编码(默认: utf8)
    - tableName: 初始化 BaseDao 对象的数据库表名(默认: None), 如果为空，
    则会初始化该数据库下所有表的信息, 如果不为空，则只初始化传入的 tableName 的表
    """
    def __init__(self, creator=pymysql, host="localhost",port=3306, user=None, password=None,
                    database=None, charset="utf8", tableName=None):
        if host is None: raise Exception("Parameter [host] is None.")
        if port is None: raise Exception("Parameter [port] is None.")
        if user is None: raise Exception("Parameter [user] is None.")
        if password is None: raise Exception("Parameter [password] is None.")
        if database is None: raise Exception("Parameter [database] is None.")
        if tableName is None: print("WARNING >>> Parameter [tableName] is None. All tables will be initialized.")
        logging.debug("[%s] 数据库初始化>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>开始"%(database))
        start = time.time()
        # 数据库连接配置
        self.__config = dict({
            "creator" : creator, "charset":charset, "host":host, "port":port, 
            "user":user, "password":password, "database":database
        })
        self.__database = database                      # 用于存储查询数据库
        self.__tableName = tableName                    # 用于临时存储当前查询表名
        # 初始化
        self.__init_connect()                           # 初始化连接
        self.__init_params()                            # 初始化参数
        end = time.time()
        logging.debug("[%s] 数据库初始化>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>结束"%(database))
        logging.debug("[%s] 数据库初始化成功。耗时：%d ms。"%(database, (end-start)))
        
    def __del__(self):
        '重写类被清除时调用的方法'
        if self.__cursor: self.__cursor.close()
        if self.__conn: self.__conn.close()
        logging.debug("[%s] 连接关闭。"%(self.__database))

    def __init_connect(self):
        '初始化连接'
        self.__conn = PooledDB.connect(**self.__config)
        self.__cursor = self.__conn.cursor()

    def __init_params(self):
        '初始化参数'
        self.__table_dict = {}
        self.__information_schema_columns = []
        self.__table_column_dict_list = {}
        if self.__tableName is None:
            self.__init_table_dict_list()
            self.__init__table_column_dict_list()
        else:
            self.__init_table_dict(self.__tableName)
            self.__init__table_column_dict_list()
            self.__column_list = self.__table_column_dict_list[self.__tableName]

    def __init__information_schema_columns(self):
        "查询 information_schema.`COLUMNS` 中的列"
        sql =   """ SELECT COLUMN_NAME 
                    FROM information_schema.`COLUMNS`
                    WHERE TABLE_SCHEMA='information_schema' AND TABLE_NAME='COLUMNS'
                """
        result_tuple = self.__exec_query(sql)
        column_list = [r[0] for r in result_tuple]
        self.__information_schema_columns = column_list

    def __init_table_dict(self, tableName):
        '初始化表'
        if not self.__information_schema_columns:
            self.__init__information_schema_columns()
        stitch_str = stitch_sequence(self.__information_schema_columns)
        sql =   """ SELECT %s FROM information_schema.`COLUMNS`
                    WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'
                """ %(stitch_str, self.__database, tableName)
        column_tuple = self.__exec_query(sql)
        column_dict = {}
        for vs in column_tuple:
            d = {k:v for k,v in zip(self.__information_schema_columns, vs)}
            column_dict[d["COLUMN_NAME"]] = d
        self.__table_dict[tableName] = column_dict

    def __init_table_dict_list(self):
        "初始化表字典对象"
        if not self.__information_schema_columns:
            self.__init__information_schema_columns()
        stitch_str = stitch_sequence(self.__information_schema_columns)
        sql1 =  """
                SELECT TABLE_NAME FROM information_schema.`TABLES` WHERE TABLE_SCHEMA='%s'
                """ %(self.__database)
        table_tuple = self.__exec_query(sql1)
        self.__table_dict = {t[0]:{} for t in table_tuple}
        for table in table_tuple:
            self.__init_table_dict(table[0])

    def __init__table_column_dict_list(self):
        '''初始化表字段字典列表'''
        for table, column_dict in self.__table_dict.items():
            column_list = [column for column in column_dict.keys()]
            self.__table_column_dict_list[table] = column_list
        
    def __exec_query(self, sql, single=False):
        '''执行查询 SQL 语句
        - @sql    查询 sql
        - @single 是否查询单个结果集，默认False
        '''
        try:
            logging.debug("[%s] SQL >>> [%s]"%(self.__database, sql))
            self.__cursor.execute(sql)
            if single:
                result_tuple = self.__cursor.fetchone()
            else:
                result_tuple = self.__cursor.fetchall()
            return result_tuple
        except Exception as e:
            print(e)

    def __exec_update(self, sql):
        '''执行更新 SQL 语句'''
        try:
            # 获取数据库游标
            logging.debug("[%s] SQL >>> [%s]"%(self.__database, sql))
            result = self.__cursor.execute(sql)
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
        '获取表对应的主键字段'
        if self.__table_dict.get(tableName) is None: raise Exception(tableName, "is not exist.")
        for column, column_dict in self.__table_dict[tableName].items():
            if column_dict["COLUMN_KEY"] == "PRI": return column

    def __get_table_column_list(self, tableName=None):
        '查询表的字段列表, 将查询出来的字段列表存入 __fields 中'
        return self.__table_column_dict_list[tableName]

    def __check_tableName(self, tableName):
        '''验证 tableName 参数'''
        if tableName is None:
            if self.__tableName is None:
                raise Exception("Parameter [tableName] is None.")
        else:
            if self.__tableName != tableName:
                self.__tableName = tableName
                self.__column_list = self.__table_column_dict_list[self.__tableName]

    def select_one(self, tableName=None, filters={}):
        '''查询单个对象
        - @tableName 表名
        - @filters 过滤条件
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_tableName(tableName)
        FIELDS = stitch_sequence(self.__get_table_column_list(self.__tableName))
        sql = "SELECT %s FROM %s"%(FIELDS ,self.__tableName)
        sql = QueryUtil.query_sql(sql, filters)
        result = self.__exec_query(sql, single=True)
        return self.__parse_result(result) 

    def select_pk(self, tableName=None, primaryKey=None):
        '''按主键查询
        - @tableName 表名
        - @primaryKey 主键值
        '''
        self.__check_tableName(tableName)
        FIELDS = stitch_sequence(self.__get_table_column_list(self.__tableName))
        sql = "SELECT %s FROM %s"%(FIELDS, self.__tableName)
        sql = QueryUtil.query_sql(sql, {self.__getpk(tableName):primaryKey})
        result = self.__exec_query(sql, single=True)
        return self.__parse_result(result)
        
    def select_all(self, tableName=None, filters={}):
        '''查询所有
        - @tableName 表名
        - @filters 过滤条件
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_tableName(tableName)
        FIELDS = stitch_sequence(self.__get_table_column_list(self.__tableName))
        sql = "SELECT %s FROM %s"%(FIELDS ,self.__tableName)
        sql = QueryUtil.query_sql(sql, filters)
        results = self.__exec_query(sql)
        return self.__parse_results(results)

    def count(self, tableName=None):
        '''统计记录数'''
        self.__check_tableName(tableName)
        sql = "SELECT count(*) FROM %s"%(self.__tableName)
        result = self.__exec_query(sql, single=True)
        return result[0]

    def select_page(self, tableName=None, page=None, filters={}):
        '''分页查询
        - @tableName 表名
        - @return 返回字典集合，集合中以表字段作为 key，字段值作为 value
        '''
        self.__check_tableName(tableName)
        if page is None:
            page = Page()
        filters["page"] = page
        FIELDS = stitch_sequence(self.__get_table_column_list(self.__tableName))
        sql = "SELECT %s FROM %s"%(FIELDS ,self.__tableName)
        sql = QueryUtil.query_sql(sql, filters)
        result_tuple = self.__exec_query(sql)
        return self.__parse_results(result_tuple)

    def save(self, tableName=None, obj=dict()):
        '''保存方法
        - @param tableName 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self.__check_tableName(tableName)
        FIELDS = stitch_sequence(seq=obj.keys())
        VALUES = []
        for k, v in obj.items():
            if self.__table_dict[tableName][k]["COLUMN_KEY"] != "PKI":
                if v is None:
                    v = "null"
                else:
                    v = '"%s"'%v
            VALUES.append(v)
        VALUES = stitch_sequence(seq=VALUES)
        sql = ' INSERT INTO `%s` (%s) VALUES(%s)'%(self.__tableName, FIELDS, VALUES)
        return self.__exec_update(sql)
    
    def update(self, tableName=None, obj={}):
        '''更新方法(根据主键更新，包含空值)
        - @param tableName 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self.__check_tableName(tableName)
        l = []
        where = "WHERE "
        for k, v in obj.items():
            if self.__table_dict[tableName][k]["COLUMN_KEY"] != "PRI":
                if v is None:
                    if self.__table_dict[tableName][k]["IS_NULLABLE"] == "YES":
                        l.append("%s=null"%(k))
                    else:
                        l.append("%s=''"%(k))
                else:
                    l.append("%s='%s'"%(k, v))
            else:
                where += "%s='%s'"%(k, v)
        sql = "UPDATE `%s` SET %s %s"%(self.__tableName, stitch_sequence(l), where)
        return self.__exec_update(sql)

    def update_selective(self, tableName=None, obj={}):
        '''更新方法(根据主键更新，不包含空值)
        - @param tableName 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self.__check_tableName(tableName)
        where = "WHERE "
        l = []
        for k, v in obj.items():
            if self.__table_dict[tableName][k]["COLUMN_KEY"] != "PRI":
                if v is None:
                    continue
                l.append("%s='%s'"%(k, v))
            else:
                where += "%s='%s'"%(k, v)
        sql = "UPDATE `%s` SET %s %s"%(self.__tableName, stitch_sequence(l), where)
        return self.__exec_update(sql)
    
    def remove(self, tableName=None, obj={}):
        '''删除方法（根据主键删除）
        - @param tableName 表名
        - @param obj 对象
        - @return 影响行数
        '''
        self.__check_tableName(tableName)
        pk = self.__getpk(self.__tableName)
        sql = "DELETE FROM `%s` WHERE %s=%s"%(self.__tableName, pk, obj[pk])
        print(sql)
        # return self.__exec_update(sql)

class Page(object):
    '分页对象'
    def __init__(self, pageNum=1, pageSize=10, count=False):
        '''
        Page 初始化方法
        - @param pageNum 页码，默认为1
        - @param pageSize 页面大小, 默认为10
        - @param count 是否包含 count 查询
        '''
        self.pageNum = pageNum if pageNum > 0 else 1            # 当前页数
        self.pageSize = pageSize if pageSize > 0 else 10        # 分页大小
        self.total = 0                                          # 总记录数
        self.pages = 1                                          # 总页数
        self.startRow = (self.pageNum - 1 ) * self.pageSize     # 起始行（用于 mysql 分页查询）
        self.endRow = self.startRow + self.pageSize             # 结束行（用于 mysql 分页查询）

class QueryUtil(object):
    '''
    SQL 语句拼接工具类：
    - 主方法：querySql(sql, filters)
    - 参数说明：   
    - @param sql：需要拼接的 SQL 语句
    - @param filters：拼接 SQL 的过滤条件 \n
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
        s = " WHERE 1=1"
        for k, v in filters.items():
            if k.startswith(QueryUtil.IN):                  # 拼接 in
                s += " AND %s IN (" %(k[4:])
                values = v.split(",")
                for value in values:
                    s += " %s,"%value
                s = s[0:len(s)-1] + ") "
            elif k.startswith(QueryUtil.NE_IN):             # 拼接 not in
                s += " AND %s NOT IN (" %(k[4:])
                values = v.split(",")
                for value in values:
                    s += " %s,"%value
                s = s[0:len(s)-1] + ") "
            elif k.startswith(QueryUtil.LIKE):              # 拼接 like
                s += " AND %s LIKE '%%%s%%' " %(k[6:], v)
            elif k.startswith(QueryUtil.LEFT_LIKE):         # 拼接左 like
                s += " AND %s LIKE '%%%s%' " %(k[6:], v)
            elif k.startswith(QueryUtil.RIGHT_LIKE):        # 拼接右 like
                s += " AND %s LIKE '%%s%%' " %(k[6:], v)
            elif k.startswith(QueryUtil.NE):                # 拼接不等于
                s += " AND %s != '%s' " %(k[4:], v)
            elif k.startswith(QueryUtil.LT):                # 拼接小于
                s += " AND %s < '%s' " %(k[4:], v)
            elif k.startswith(QueryUtil.LE):                # 拼接小于等于
                s += " AND %s <= '%s' " %(k[4:], v)
            elif k.startswith(QueryUtil.GT):                # 拼接大于
                s += " AND %s > '%s' " %(k[4:], v)
            elif k.startswith(QueryUtil.GE):                # 拼接大于等于
                s += " AND %s >= '%s' " %(k[4:], v)
            else:                                           # 拼接等于
                if isinstance(v, str):
                    s += " AND %s='%s' "%(k, v)
                elif isinstance(v, int):
                    s += " AND %s=%d "%(k, v)
        return s

    @staticmethod
    def __filter_group(filters):
        '''过滤分组'''
        group = filters.pop(QueryUtil.GROUP)
        s = " GROUP BY %s"%(group)
        return s

    @staticmethod
    def __filter_order(filters):
        '''过滤排序'''
        order = filters.pop(QueryUtil.ORDER)
        type = filters.pop(QueryUtil.ORDER_TYPE)
        s = " ORDER BY `%s` %s"%(order, type)
        return s

    @staticmethod
    def __filter_page(filters):
        '''过滤 page 对象'''
        page = filters.pop("page")
        return " LIMIT %d,%d"%(page.startRow, page.endRow)
        
    @staticmethod
    def query_sql(sql=None, filters=dict()):
        '''拼接 SQL 查询条件
        - @param sql SQL 语句
        - @param filters 过滤条件
        - @return 返回拼接 SQL
        '''
        if not filters:
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

    @staticmethod
    def query_set(fields, values):
        s = " SET "
        for f, v in zip(fields, values):
            s += '%s="%s", '
        pass

def test():
    config = {
        # "creator": pymysql,
        # "host" : "127.0.0.1", 
        "user" : "root", 
        "password" : "root",
        "database" : "py", 
        # "port" : 3306,
        # "charset" : 'utf8'
        # "tableName" : "fake",
    }
    base = BaseDao(**config)
    ########################################################################
    # fake = base.select_one("fake")
    # print(fake)
    ########################################################################
    # users = base.select_all("fake")
    # print(users)
    ########################################################################
    # filter1 = {
    #     "status":1,
    #     "_in_id":"1,2,3,4,5",
    #     "_like_name":"zhang",
    #     "_ne_name":"wangwu"
    # }
    # user_filters = base.select_all("user", filter1)
    # print(user_filters)
    ########################################################################
    # role = base.select_one("role")
    # print(role)
    ########################################################################
    # fake = base.select_pk("fake", 2)
    # print(fake)
    # base.update("fake", fake)
    # base.update_selective("fake", fake)
    # base.remove("fake", fake)
    ########################################################################
    # user_limit = base.select_page("user")
    # print(user_limit)
    ########################################################################
    # fake = {
    #     "id": "null",
    #     "name": "test",
    #     "value": "test"
    # }
    # flag = base.save("fake", fake)
    # print(flag)

if __name__ == "__main__":
    test()
