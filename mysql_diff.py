import pymysql
import sys


class TableInfo:
    def __init__(self, table_name, name, column_type, nullable, default, comment, position):
        self.table_name = table_name
        self.name = name
        self.column_type = column_type
        self.nullable = "NULL" if nullable == "YES" else "NOT NULL"
        self.default = "" if default is None else default
        self.comment = "" if comment is None else comment
        self.position = position
        self.core = (name, column_type, nullable)
        self.before_name = ""

    def print_info(self):
        return "[{}, {}, {}, {}, {}]".format(self.name, self.column_type, self.nullable, self.default, self.comment)

    def print_add_sql(self):
        return "alter table {} add column {} {} {} default '{}' comment '{}' after {}" \
            .format(self.table_name, self.name,
                    self.column_type,
                    self.nullable, self.default,
                    self.comment,
                    self.before_name)

    def print_change_sql(self):
        return "alter table {} change column {} {} {} {} default '{}' comment '{}'" \
            .format(self.table_name, self.name,
                    self.name,
                    self.column_type,
                    self.nullable, self.default,
                    self.comment)


def get_table_list(host, port, user, password, database):
    db = pymysql.connect(host=host, port=port, user=user, password=password, database=database)
    cursor = db.cursor()

    cursor.execute("SELECT TABLE_NAME FROM information_schema.`TABLES` where TABLE_SCHEMA = '" + database + "';")
    table_list = cursor.fetchall()

    param = {}
    for table in table_list:
        cursor.execute(
            "SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_COMMENT, ORDINAL_POSITION"
            + " FROM information_schema.`COLUMNS` where TABLE_SCHEMA = '"
            + database + "' and TABLE_NAME = '"
            + table[0] + "';")
        result = cursor.fetchall()
        table_info_list = []
        for i in result:
            table_info_list.append(TableInfo(i[0], i[1], i[2], i[3], i[4], i[5], i[6]))
        for i in range(1, len(table_info_list)):
            table_info_list[i].before_name = table_info_list[i - 1].name
        param[table[0]] = table_info_list

    cursor.close()
    db.close()

    return param


def show_diff(base, change):
    to_add_table_list, to_drop_table_list = [], []
    to_add_column_map, to_drop_column_map, to_change_column_map = {}, {}, {}

    for table, params in change.items():
        if table not in base:
            print("to drop table: " + table)
            to_drop_table_list.append(table)
    for table, params in base.items():
        if table not in change:
            print("to add table: " + table)
            to_add_table_list.append(table)
    for table, params in base.items():
        if table in change:
            old_params = change[table]
            old_column = [i.core for i in old_params]
            new_column = [i.core for i in params]
            if new_column != old_column:
                old_map, new_map = {}, {}
                for i in old_params:
                    old_map[i.name] = i
                for i in params:
                    new_map[i.name] = i
                print("------------------------------" + table + "------------------------------")
                print("to add column: " + str(list(new_map.keys() - old_map.keys())))
                print("to drop column: " + str(list(old_map.keys() - new_map.keys())))
                to_drop_column_map[table] = [old_map[i] for i in list(old_map.keys() - new_map.keys())]
                to_add_column_map[table] = [new_map[i] for i in list(new_map.keys() - old_map.keys())]
                print("to change column: ")

                to_change = list(set(new_column) - set(old_column))
                to_change_info_list = []
                for i in to_change:
                    if i[0] in old_map:
                        print(str(old_map[i[0]].print_info()) + " -> " + str(new_map[i[0]].print_info()))
                        to_change_info_list.append(new_map[i[0]])
                to_change_column_map[table] = to_change_info_list

    return to_add_table_list, to_drop_table_list, to_add_column_map, to_drop_column_map, to_change_column_map


def gen_add_table_sql(table_list):
    pass


def gen_drop_table_sql(table_list):
    for table in table_list:
        print("drop table " + table)


def gen_add_column_sql(column_map):
    for k, v in column_map.items():
        for column in v:
            print(column.print_add_sql())


def gen_drop_column_sql(column_map):
    for k, v in column_map.items():
        for column in v:
            print("alter table " + k + " drop column " + column.name + ";")


def gen_change_column_sql(column_map):
    for k, v in column_map.items():
        for column in v:
            print(column.print_change_sql())


if __name__ == '__main__':
    database = sys.argv[1]
    print("------------------------" + database + "------------------------")
    # write your own mysql info
    test_table_params = get_table_list('xx', 3306, 'xx', 'xx', database)
    prod_table_params = get_table_list('xx', 3306, 'xx', 'xx', database)
    a, b, c, d, e = show_diff(test_table_params, prod_table_params)
    print("------------------------------------------------------------")
    gen_add_table_sql(a)
    gen_drop_table_sql(b)
    gen_add_column_sql(c)
    gen_drop_column_sql(d)
    gen_change_column_sql(e)
