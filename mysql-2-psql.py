#! python3

import pymysql

class Mysql(object):
    def __init__(self, host, user, pwd, db):
        self.conn = pymysql.connect(host=host, user=user, password=pwd, db=db)
        self.cursor = self.conn.cursor()

    def insert(self, sql):
        #print(sql)
        self.cursor.execute(sql)
        self.conn.commit()

    def insert(self, sql, param):
        #print(sql, param)
        self.cursor.execute(sql, param)
        self.conn.commit()

    def insert_batch(self, sql, param):
        #print(sql, param)
        self.cursor.executemany(sql, param)
        self.conn.commit()

    def select(self, sql):
        #print(sql)
        self.cursor.execute(sql)
        data = self.cursor.fetchall()
        return data

    def update(self, sql):
        # print(sql)
        self.cursor.execute(sql)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()

class MysqlTable():
    def __init__(self, table_sql):
        self.name = ""
        self.comment = ""
        self.pk = []
        self.uk = []
        self.key = []
        self.fields = []

        lines = table_sql.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith("CREATE TABLE"):
                self.table_name(line)
            elif line.startswith("PRIMARY KEY"):
                self.pk_key(line)
            elif line.startswith("UNIQUE KEY"):
                self.uk_key(line)
            elif line.startswith("KEY"):
                self.set_key(line)
            elif line.startswith("`"):
                self.field(line)
            else:
                self.last(line)

    def table_name(self, line):
        """CREATE TABLE `t_xxxx`"""
        strs = line.split(" ")
        self.name = self.get_field(strs[2])

    def pk_key(self, line):
        """PRIMARY KEY (`id`,`id2`)"""
        strs = line.split(" ")
        fields = strs[2].replace("(", "").replace(")", "")
        for f in fields.split(","):
            if len(f) != 0:
                self.pk.append(self.get_field(f))

    def uk_key(self, line):
        """UNIQUE KEY `unique_key_xxxx` (`a`,`b`,`c`,`d`),"""
        strs = line.split(" ")
        uk = []
        fields = strs[3].replace("(", "").replace(")", "")
        for f in fields.split(","):
            if len(f) != 0:
                uk.append(self.get_field(f))
        self.uk.append(uk)

    def set_key(self, line):
        """ KEY `idx_xxx` (`x`,`xx`) """
        strs = line.split(" ")
        key = []
        fields = strs[2].replace("(", "").replace(")", "")
        for f in fields.split(","):
            if len(f) != 0:
                key.append(self.get_field(f))
        self.key.append(key)

    def field(self, line):
        col = {}
        strs = line.split(" ")
        # field
        col['name'] = self.get_field(strs[0])
        # type
        col['type'] = self.get_field(strs[1])
        col['unsigned'] = False
        if line.find("%s %s" % (col['type'], "unsigned")) != -1:
            col['unsigned'] = True
        # null or not
        col["null_able"] = True
        if line.find("NOT NULL") != -1:
            col["null_able"] = False
        # default
        col['auto_inc'] = False
        if line.find("AUTO_INCREMENT") != -1:
            col['auto_inc'] = True
        else:
            i = line.find("DEFAULT")
            if i != -1:
                d_subs = line[i:].split(" ")
                col['default'] = self.get_desc(d_subs[1])
        # COMMENT
        i = line.find("COMMENT")
        if i != -1:
            c_subs = line[i:].replace("COMMENT", "").replace(",", "").strip()
            col['comment'] = self.get_desc(c_subs)
        # 自更新
        col['upd_time'] = False
        if line.find("URRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP") !=-1:
            col['upd_time'] = True

        self.fields.append(col)

    def last(self, line):
        i = line.find("COMMENT=");
        if i != -1:
            self.comment = self.get_desc(line[i:])

    def get_field(self, s):
        f = s.replace("`", "").replace("`", "")
        return f

    def get_desc(self, s):
        f = s.replace("'", "").replace("'", "")
        return f

class PsqlTabSql:
    def __init__(self):
        self.mysql_psql_type_map = {}
        self.mysql_psql_unsigned_type_map = {}
        self.init_type_map()

    def init_type_map(self):
        # add your care mysql type to psql type
        self.mysql_psql_type_map['tinyint'] = 'smallint'
        self.mysql_psql_type_map['smallint'] = 'smallint'
        self.mysql_psql_type_map['mediumint'] = 'integer'
        self.mysql_psql_type_map['int'] = 'integer'
        self.mysql_psql_type_map['bigint'] = 'bigint'
        self.mysql_psql_type_map['float'] = 'real'
        self.mysql_psql_type_map['double'] = 'double precision'
        self.mysql_psql_type_map['boolean'] = 'boolean'

        self.mysql_psql_type_map['tinytext'] = 'text'
        self.mysql_psql_type_map['text'] = 'text'
        self.mysql_psql_type_map['mediumtext'] = 'text'
        self.mysql_psql_type_map['longtext'] = 'text'

        self.mysql_psql_type_map['char'] = 'character'
        self.mysql_psql_type_map['varchar'] = 'character varying'

        self.mysql_psql_type_map['datetime'] = 'timestamp with time zone'
        self.mysql_psql_type_map['timestamp'] = 'timestamp with time zone'

        # unsigned
        self.mysql_psql_unsigned_type_map['tinyint'] = 'smallint'
        self.mysql_psql_unsigned_type_map['smallint'] = 'integer'
        self.mysql_psql_unsigned_type_map['mediumint'] = 'integer'
        self.mysql_psql_unsigned_type_map['int'] = 'bigint'
        # self.mysql_psql_unsigned_type_map['bigint'] = 'numeric(20)'
        self.mysql_psql_unsigned_type_map['bigint'] = 'bigint'
        self.mysql_psql_unsigned_type_map['float'] = 'real'
        self.mysql_psql_unsigned_type_map['double'] = 'double precision'

    def build_psql_tabel(self, tab):
        tab_str = "drop table if exists %s;\n" % tab.name
        tab_str = tab_str + "CREATE TABLE %s ( \n" % tab.name

        # field
        for field_attr in tab.fields:
            tab_str = tab_str + "\t%s,\n" % self.get_col_define(field_attr)
        # pk
        tab_str = tab_str + "CONSTRAINT pk_%s PRIMARY KEY (%s),\n" % (tab.name, ",".join(tab.pk))
        # uk
        for uk in tab.uk:
            tab_str = tab_str + "CONSTRAINT uk_%s_%s unique(%s),\n" % (tab.name, "_".join(uk), ",".join(uk))
        tab_str = tab_str[:len(tab_str)-2] + "\n);\n"

        # key
        for key in tab.key:
            tab_str = tab_str + "create index idx_%s_%s on %s (%s);\n" % (tab.name, "_".join(key), tab.name, ",".join(key))

        # comment
        for f in tab.fields:
            if 'comment' in f.keys():
                tab_str = tab_str + "comment  on column %s.%s is '%s';\n" % (tab.name, f['name'], f['comment'])

        if len(tab.comment) != 0:
            tab_str = tab_str + "comment on table %s is '%s';\n" % (tab.name, tab.comment)

        has_update_time = False
        for f in tab.fields:
            if f['upd_time']:
                has_update_time = True
                break

        if has_update_time:
            tab_str = tab_str + "create trigger trg_%s before update on %s for each row execute procedure " \
                                "upd_timestamp();\n" % (tab.name, tab.name)

        return tab_str

    def get_col_define(self, col):
        if col['auto_inc']:
            if col['type'] == 'bigint':
                return "%s bigserial" % col['name']
            else:
                return "%s serial" % col['name']
        return "%s %s%s%s" % (col['name'],
                               self.get_type(col),
                               self.get_null_or_not(col),
                               self.get_default(col))

    def get_type(self, col):
        f_type = col['type'].lower()
        i = f_type.find("(")
        suffix = ""
        if i != -1:
            suffix = f_type[i:]
            f_type = f_type[:i]
        real_type = self.mysql_psql_unsigned_type_map[f_type] if col['unsigned'] else self.mysql_psql_type_map[f_type]
        if real_type is None:
            raise Exception('not find type map :' + f_type)

        if (real_type == "real" or real_type == "double") and len(suffix) != 0:
            return "numeric%s" % suffix

        if f_type == "char" or f_type == "varchar":
            return "%s%s" % (real_type, suffix)
        else:
            return real_type


    def get_null_or_not(self, col):
        return "" if col['null_able'] else " not null"

    def get_default(self, col):
        if 'default' in col.keys():
            if col['default'].lower() != 'null':
                val = col['default']
                if val != "CURRENT_TIMESTAMP":
                    val = "'%s'" % val
                return " default %s" % val
        return ""

if __name__ == '__main__':
    mysql = Mysql(host='127.0.0.1', user='root', pwd='root', db='test')

    upd_trigger = '''
create or replace function upd_timestamp() returns trigger as
$$
begin
    new.updatetime = current_timestamp; -- "updatetime" is your update time field name
    return new;
end
$$
language plpgsql;
'''
    file = open("out.sql", mode='w', encoding='utf8')
    file.write(upd_trigger)

    tables = mysql.select("show tables")
    for row in tables:
        table = row[0]
        print(table)
        tab_sql = mysql.select("show create table %s" % table)
        # print(tab_sql[0][1])
        tab = MysqlTable(tab_sql[0][1])
        # print(tab.pk)
        # print(tab.uk)
        # print(tab.name)
        # for f in tab.fields:
        #     print(f)

        psql = PsqlTabSql()
        tab_str = psql.build_psql_tabel(tab)
        # print(tab_str)
        file.write(tab_str)

    file.flush()
    file.close()
    mysql.close()
