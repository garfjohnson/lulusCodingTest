import pandas as pd
from sqlalchemy import create_engine
import psycopg2


CREATE_CATEGORIES_TABLE = """CREATE TABLE IF NOT EXISTS categories (
category_id INTEGER, category_name TEXT);"""
CREATE_EMPLOYEES_TABLE = """CREATE TABLE IF NOT EXISTS employees (
id INTEGER, first_name TEXT, last_name TEXT, email TEXT, gender TEXT,
ip_address TEXT, hire_date TEXT
);"""
CREATE_ORDER_PRODUCTS_TABLE = """CREATE TABLE IF NOT EXISTS order_products(
order_number INTEGER, product_id INTEGER, quantity INTEGER
);"""
CREATE_ORDERS_TABLE = """CREATE TABLE IF NOT EXISTS orders(
order_number INTEGER, order_date TEXT, customer_id INTEGER,
shipping_cost REAL, discount_amount TEXT
);"""
CREATE_PRODUCTS_TABLE = """CREATE TABLE IF NOT EXISTS products(
product_id INTEGER, manufacturer TEXT, sku TEXT, name TEXT, price REAL,
stock INTEGER, category_id TEXT, lf_code TEXT
);"""
CREATE_PROJECTS_TABLE = """CREATE TABLE IF NOT EXISTS projects(
employee_id INTEGER, project_id INTEGER, project_hours INTEGER
);"""
CREATE_SALARY_TABLE = """CREATE TABLE IF NOT EXISTS salary(
employee_id INTEGER, salary INTEGER
);"""

##use pscyopg2 to connect and create tables
def create_tables(connection):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_CATEGORIES_TABLE)
            cursor.execute(CREATE_EMPLOYEES_TABLE)
            cursor.execute(CREATE_ORDER_PRODUCTS_TABLE)
            cursor.execute(CREATE_ORDERS_TABLE)
            cursor.execute(CREATE_PRODUCTS_TABLE)
            cursor.execute(CREATE_PROJECTS_TABLE)
            cursor.execute(CREATE_SALARY_TABLE)


## load csvs directly from github into pandas dfs
categories_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/categories.csv'
categories_df = pd.read_csv(categories_url, error_bad_lines=False).set_index('category_id')

employees_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/employees.csv'
employees_df = pd.read_csv(employees_url, error_bad_lines=False).set_index('id')

order_products_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/order_products.csv'
order_products_df = pd.read_csv(order_products_url, error_bad_lines=False).set_index('order_number')

orders_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/orders.csv'
orders_df = pd.read_csv(orders_url, error_bad_lines=False).set_index('order_number')

products_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/products.csv'
products_df = pd.read_csv(products_url, error_bad_lines=False).set_index('product_id')

projects_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/projects.csv'
projects_df = pd.read_csv(projects_url, error_bad_lines=False).set_index('employee_id')

salary_url = 'https://raw.githubusercontent.com/BigEarl47/lulus_etl/main/data_test/data_files/salary.csv'
salary_df = pd.read_csv(salary_url, error_bad_lines=False).set_index('employee_id')



## connect to postgreSQL db
database_url = YOUR_DATABASE_URL
connection = psycopg2.connect(database_url)
##create tables
create_tables(connection)

# ##insert dfs
engine = create_engine(database_url)
## use SQLalchemy to insert df into postgres
categories_df.to_sql('categories', engine, if_exists='append')
employees_df.to_sql('employees', engine, if_exists='append')
order_products_df.to_sql('order_products', engine, if_exists='append')
orders_df.to_sql('orders', engine, if_exists='append')
products_df.to_sql('products', engine, if_exists='append')
projects_df.to_sql('projects', engine, if_exists='append')
salary_df.to_sql('salary', engine, if_exists='append')



