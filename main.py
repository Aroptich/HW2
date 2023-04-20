import pymysql

from config import  host, port, user, password, db_name


class Database:

    def __init__(self, host, port, user, password, db_name):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db_name = db_name

    @staticmethod
    def connect(func,):
        def wrapper(self, *args, **kwargs):
            try:
                connection = pymysql.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=db_name,
                    cursorclass=pymysql.cursors.DictCursor
                )
                print("successfully connected....")
                with connection.cursor() as cursor:
                    cursor.execute(func(self, *args, **kwargs))
                    connection.commit()
            except Exception as err:
                print("Connection refused...")
                print(err)
            finally:
                connection.close()

        return wrapper

    @staticmethod
    def reading_data(func):
        def wrapper(self, *args, **kwargs):
            try:
                connection = pymysql.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    database=db_name,
                    cursorclass=pymysql.cursors.DictCursor
                )
                print("successfully connected....")
                with connection.cursor() as cursor:
                    cursor.execute(func(self, *args, **kwargs))
                    rows = cursor.fetchall()
                    print([row for row in rows])
                    connection.commit()
            except Exception as err:
                print("Connection refused...")
                print(err)
            finally:
                connection.close()

        return wrapper

    @connect
    def create_table(self, table_name: str, **dict_data: dict) -> str:
        try:
            self.table_name = table_name
            self.columns_name = [keys for keys in dict_data]
            self.type_data = [dict_data[keys] for keys in dict_data]
            self.res = ','.join([' '.join(i) for i in zip(self.columns_name, self.type_data)])
            self.create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.res});"
            return self.create_table_query
        except Exception as err:
            print(err)

    @connect
    def insert_data(self, table_name: str,  **kwargs):
        try:
            self.table_name = table_name
            self.columns_name = ','.join([keys for keys in kwargs])
            self.values = ','.join([str(kwargs[keys]) for keys in kwargs])
            self.insert_data_query = f"INSERT INTO {self.table_name} ({self.columns_name}) VALUES ({self.values});"
            return self.insert_data_query
        except Exception as err:
            print(err)

    @reading_data
    def order_type_sales(self, table_name: str, *args):
        try:
            self.table_name = table_name
            self.count_product, self.id, = args
            self.select_order_type = f"SELECT {self.id} AS 'id заказа'," \
                                     f"CASE " \
                                     f"WHEN {self.count_product}  < 100 THEN 'Маленький заказ'" \
                                     f"WHEN {self.count_product}  between 100 AND 300 THEN 'Средний заказ'" \
                                     f"WHEN {self.count_product}  > 300 THEN 'Большой заказ'" \
                                     f"ELSE 'Не определено'" \
                                     f"END AS 'Тип заказа'" \
                                     f"FROM {self.table_name};"
            return self.select_order_type
        except Exception as err:
            print(err)

    @reading_data
    def order_type_orders(self,table_name: str, *args, **kwargs):
        try:
            self.table_name = table_name
            self.id, self.employee_id, self.amount, self.order_status, = args
            self.select_order_type = f"SELECT {self.id}," \
                                     f"{self.employee_id}," \
                                     f"{self.amount}," \
                                     f"CASE " \
                                     f"WHEN {self.order_status}  = 'OPEN' THEN 'Order is in open state'" \
                                     f"WHEN {self.order_status}  = 'CLOSED' THEN 'Order is closed'" \
                                     f"WHEN {self.order_status}  = 'CANCELLED' THEN 'Order is cancelled'" \
                                     f"ELSE 'Не определено'" \
                                     f"END AS 'full_order_status'" \
                                     f"FROM {self.table_name};"
            return self.select_order_type
        except Exception as err:
            print(err)

    @connect
    def select_data(self, table_name: str, **kwargs):
        try:
            self.name_table = table_name
            self.select_all_rows = f"SELECT * FROM {self.name_table}"
            self.rows = self.cursor.fetchall()
            for row in self.rows:
                print(row)
        except Exception as err:
            print(err)

    @reading_data
    def show_tables(self, *args, **kwargs):
        try:
            self.select_all_tables = f"SHOW TABLES"
            return self.select_all_tables
        except Exception as err:
            print(err)
    @connect
    def rename_table(self, old_table_name: str, new_table_name: str):
        try:
            self.old_table_name = old_table_name
            self.new_table_name = new_table_name
            self.new_name_table = f"RENAME TABLE {self.old_table_name} TO {self.new_table_name};"
            return self.new_name_table
        except Exception as err:
            print(err)


    def update_table(self):
        pass

    @connect
    def drop_table(self, table_name: str):
        try:
            self.table_name = table_name
            self.drop_table = f"DROP TABLE {self.table_name}"
            print(f"Table '{self.table_name}' deleted")
            return self.drop_table
        except Exception as err:
            print(err)


if __name__ == '__main__':
    db = Database(host,port,user,password,db_name)
    create_table = db.create_table('sales',
                                   id='int auto_increment primary key',
                                   order_date='date',
                                   count_product='int')
    create_table = db.create_table('orders',
                                           id='int auto_increment primary key',
                                           employee_id='text',
                                           amount='double',
                                           order_status='text')
    insert_table_sales = db.insert_data('sales', order_date='"2020-01-01"', count_product=156)
    insert_table_sales = db.insert_data('sales', order_date='"2020-01-02"', count_product=180)
    insert_table_sales = db.insert_data('sales', order_date='"2020-01-03"', count_product=21)
    insert_table_sales = db.insert_data('sales', order_date='"2020-01-04"', count_product=124)
    insert_table_sales = db.insert_data('sales', order_date='"2020-01-05"', count_product=341)

    insert_table_orders = db.insert_data('orders', employee_id='"e03"', amount=15.00, order_status='"OPEN"')
    insert_table_orders = db.insert_data('orders', employee_id='"e01"', amount=25.50, order_status='"OPEN"')
    insert_table_orders = db.insert_data('orders', employee_id='"e05"', amount=100.70, order_status='"CLOSED"')
    insert_table_orders = db.insert_data('orders', employee_id='"e02"', amount=22.18, order_status='"OPEN"')
    insert_table_orders = db.insert_data('orders', employee_id='"e04"', amount=9.50, order_status='"CANCELLED"')

    select_table_sales = db.select_data('sales')
    select_table_orders = db.select_data('orders')
    show_tables = db.show_tables()
    rename_table = db.rename_table('sales', 'new_sales')
    show_tables = db.show_tables()

    order_type = db.order_type_sales('`sales`', '`count_product`', '`id`')

    order_type_orders = db.order_type_orders('`orders`', '`id`', '`employee_id`', '`amount`', '`order_status`')
    drop_table = db.drop_table('orders')
    show_tables = db.show_tables()
