import asyncio
import logging
import aiomysql; logging.basicConfig(level=logging.INFO)
from attr import attr
from sympy import false


async def createPool(loop, **kw):
    logging.info("create database connection pool...")
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get("host", "localhost"),
        port=kw.get("port", 3306),
        user=kw["user"],
        password=kw["password"],
        db=kw["db"],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )

async def select(query, bvs, size=None):
    logging.info("execute", query, bvs)
    global __pool
    with await __pool as conn:
        c = await conn.cursor(aiomysql.DictCursor)
        await c.execute(query.replace("?", "%s"), bvs or ())
        if size:
            result = await c.fetchmany(size)
        else:
            result = await c.fetchall()
        
        await c.close()
        logging.info('rows returned: %s' % len(result))
        return result

async def execute(sql, args):
    logging.info("execute", sql, args)
    global __pool
    with await __pool as conn:
        try:
            c = await conn.cursor()
            result = await c.execute(sql.replace("?", "%s"), args or ())
            await c.close()
            return result
        except BaseException as e:
            logging.info("error happen when execute %s, %s" % (sql, e))
            raise

def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)

class Field(object):
    def __init__(self, name, column_type, primary_key, default) -> None:
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
    
    def __str__(self) -> str:
        return "<%s, %s:%s>" % (self.__class__.__name__, self.name, self.column_type)

class BoolField(Field):
    def __init__(self, name=None, column_type = "bool", primary_key = false, default = None) -> None:
        super().__init__(name, column_type, primary_key, default)

class IntegerField(Field):
    def __init__(self, name=None, column_type = "int", primary_key = false, default = None) -> None:
        super().__init__(name, column_type, primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, column_type = "float", primary_key = false, default = None) -> None:
        super().__init__(name, column_type, primary_key, default)
    
class StringField(Field):
    def __init__(self, name=None, column_type = "varchar(100)", primary_key = false, default = None) -> None:
        super().__init__(name, column_type, primary_key, default)

class TextField(Field):
    def __init__(self, name=None, column_type = "text", primary_key = false, default = None) -> None:
        super().__init__(name, column_type, primary_key, default)

class ModelMetaClass(type):
    def __new__(cls, name, bases, attrs):
        if name == "Model":
            return type.__new__(cls, name, bases, attrs)
        
        tableName = attrs.get("__table__", None) or name
        logging.info("found model: %s (table: %s)" % (name, tableName))

        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info("found mapping %s ==> %s" % (k, v))
                mappings[k] = v
                if v.primary_key:
                    if primaryKey:
                        raise RuntimeError("dup primary key")
                    primaryKey = k
                else:
                    fields.append(k)
        
        if not primaryKey:
            raise RuntimeError("primary key not found")
        for k in mappings:
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings # 保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey # 主键属性名
        attrs['__fields__'] = fields # 除主键外的属性名
        # 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)




class Model(dict, metaclass = ModelMetaClass):
    def __init__(self, **kw):
        super().__init__(**kw)
    
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, k):
        return getattr(self, k, None)

    def getValueOrDefault(self, k):
        v = getattr(self, k, None)
        if v is None:
            field = self.__mappings__[k]
            if field.default is not None:
                v = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (k, str(v)))
                setattr(self, k, v)
        return v
    
    @classmethod
    async def find(cls, pk):
        ' find object by primary key. '
        rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])
    
    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        ' find objects by where clause. '
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value: %s' % str(limit))
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        ' find number by select and where. '
        sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']
    
    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('failed to update by primary key: affected rows: %s' % rows)

    async def remove(self):
        pkValue = self.getValue(self.__primary_key__)
        rows = await execute(self.__delete__, [pkValue])
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)