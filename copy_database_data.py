# -*- coding: utf-8 -*-
import pymysql
import time
import warnings

warnings.filterwarnings("ignore")


class ConnectMysql(object):
    def __init__(self):
        # 这里设置分页查询, 每页查询多少数据
        self.page_size = 5000

    def get_table(self, db_name):
        db = pymysql.connect(host='xx', port=3306, user='xx', password='xx', database=db_name)
        db_local = pymysql.connect(host='xx', port=3306, user='xx', password='xx', database=db_name)
        cur = db.cursor()
        cur_local = db_local.cursor()
        cur.execute('show tables')
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0]
            print(table_name)
            # 需要迁移的数据库查询表的列数
            cur.execute(
                f"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE table_schema='{db_name}' AND table_name='{table_name}'")
            table_col_count = cur.fetchone()
            # 需要迁移的数据库查询表的结构
            cur.execute('show create table ' + table_name)
            result = cur.fetchall()
            create_sql = result[0][1]
            # 查询需要迁移的数据库表的数据条数
            cur.execute('select count(*) from ' + table_name)
            total = cur.fetchone()[0]
            page = int(total / self.page_size)
            page1 = total % self.page_size
            if page1 != 0:
                page = page + 1
            print("一共" + str(total) + "条")
            print("一个" + str(page) + "页")

            cur_local.execute(
                f"SELECT table_name FROM information_schema.`TABLES` WHERE table_schema='{db_name}' AND table_name='{table_name}'")
            local_table_name = cur_local.fetchone()[0]
            if local_table_name is not None:
                print("表" + local_table_name + "已存在，正在drop...")
                cur_local.execute('drop table ' + local_table_name)
            cur_local.execute(create_sql)

            for p in range(0, page):
                while True:
                    try:
                        print('开始', table[0], '的第', p + 1, '页查询')
                        if p == 0:
                            limit_param = ' limit ' + str(p * self.page_size) + ',' + str(self.page_size)
                        else:
                            limit_param = ' limit ' + str(p * self.page_size + 1) + ',' + str(self.page_size)
                        cur.execute('select * from ' + table[0] + limit_param)
                        inserts = cur.fetchall()
                        print('查询成功')
                        param = ''
                        for i in range(0, table_col_count[0]):
                            param = param + '%s,'
                        print('开始插入')
                        cur_local.executemany('insert into ' + table[0] + ' values (' + param[0:-1] + ')', inserts)
                        print(table[0], '的第', p + 1, '页, 插入完成, 还有', page - p - 1, '页')
                        db_local.commit()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(60)
                        cur = db.cursor()
                        cur_local = db_local.cursor()
                print(table[0] + ' 插入完成')
                print('======================================================================== \n\n')
        cur_local.close()
        db_local.close()
        cur.close()
        db.close()


if __name__ == '__main__':
    conn_mysql = ConnectMysql()
    conn_mysql.get_table("xx")
