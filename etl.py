import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

#configuracion del archivo .env
DB = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME", "roseamor"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def get_conn():
    return psycopg2.connect(**DB)


RAW_DDL = """
CREATE SCHEMA IF NOT EXISTS raw;
DROP TABLE IF EXISTS raw.orders CASCADE;

CREATE TABLE raw.orders (
    order_id TEXT, customer_id TEXT, sku TEXT,
    quantity TEXT, unit_price TEXT, order_date TEXT, channel TEXT
);
DROP TABLE IF EXISTS raw.customers CASCADE;

CREATE TABLE raw.customers (
    customer_id TEXT, name TEXT, country TEXT, segment TEXT, created_at TEXT
);

DROP TABLE IF EXISTS raw.products CASCADE;
CREATE TABLE raw.products (sku TEXT, category TEXT, cost TEXT, active TEXT);
"""

def load_raw(conn):
    cur = conn.cursor()
    cur.execute(RAW_DDL)
    for table, path in [
        ("raw.orders",    "data/raw/orders.csv"),
        ("raw.customers", "data/raw/customers.csv"),
        ("raw.products",  "data/raw/products.csv"),
    ]:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
        rows = [tuple(r) for r in df.itertuples(index=False)]
        cols = ", ".join(df.columns)
        execute_values(cur, f"INSERT INTO {table} ({cols}) VALUES %s", rows)
    conn.commit()
    cur.close()


STAGING_DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
DROP TABLE IF EXISTS staging.orders CASCADE;
CREATE TABLE staging.orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    sku         TEXT NOT NULL,
    quantity    INTEGER NOT NULL,
    unit_price  NUMERIC(12,2) NOT NULL,
    order_date  DATE NOT NULL,
    channel     TEXT NOT NULL,
    revenue     NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
);
DROP TABLE IF EXISTS staging.customers CASCADE;
CREATE TABLE staging.customers (
    customer_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT NOT NULL,
    segment     TEXT NOT NULL,
    created_at  DATE NOT NULL
);
DROP TABLE IF EXISTS staging.products CASCADE;
CREATE TABLE staging.products (
    sku      TEXT PRIMARY KEY,
    category TEXT NOT NULL,
    cost     NUMERIC(12,2) NOT NULL,
    active   BOOLEAN NOT NULL
);
"""

STAGING_ORDERS = """
INSERT INTO staging.orders (order_id, customer_id, sku, quantity, unit_price, order_date, channel)
SELECT DISTINCT ON (order_id)
    order_id, customer_id, sku,
    quantity::INTEGER,
    unit_price::NUMERIC(12,2),
    order_date::DATE,
    lower(trim(channel))
FROM raw.orders
WHERE unit_price IS NOT NULL AND unit_price <> ''
  AND quantity::INTEGER > 0
  AND order_date ~ '^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])'
ORDER BY order_id, order_date DESC;
"""

STAGING_CUSTOMERS = """
INSERT INTO staging.customers (customer_id, name, country, segment, created_at)
SELECT
    customer_id, name,
    COALESCE(NULLIF(trim(country), ''), 'Unknown'),
    COALESCE(NULLIF(trim(segment), ''), 'Unknown'),
    created_at::DATE
FROM raw.customers;
"""

STAGING_PRODUCTS = """
INSERT INTO staging.products (sku, category, cost, active)
SELECT
    sku,
    COALESCE(NULLIF(trim(category), ''), 'Uncategorized'),
    cost::NUMERIC(12,2),
    (lower(trim(active)) = 'true')
FROM raw.products;
"""

def load_staging(conn):
    cur = conn.cursor()
    cur.execute(STAGING_DDL)
    cur.execute(STAGING_ORDERS)
    cur.execute(STAGING_CUSTOMERS)
    cur.execute(STAGING_PRODUCTS)
    conn.commit()
    cur.close()


CONSUMPTION_DDL = """
CREATE SCHEMA IF NOT EXISTS consumption;
DROP TABLE IF EXISTS consumption.fact_orders CASCADE;
DROP TABLE IF EXISTS consumption.dim_customer CASCADE;
DROP TABLE IF EXISTS consumption.dim_product CASCADE;
DROP TABLE IF EXISTS consumption.dim_date CASCADE;

CREATE TABLE consumption.dim_date (
    date_id DATE PRIMARY KEY, year SMALLINT, quarter SMALLINT,
    month SMALLINT, month_name TEXT, week SMALLINT, day SMALLINT, weekday TEXT
);
CREATE TABLE consumption.dim_customer AS SELECT * FROM staging.customers WITH NO DATA;
ALTER TABLE consumption.dim_customer ADD PRIMARY KEY (customer_id);
CREATE TABLE consumption.dim_product AS SELECT * FROM staging.products WITH NO DATA;
ALTER TABLE consumption.dim_product ADD PRIMARY KEY (sku);
CREATE TABLE consumption.fact_orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT REFERENCES consumption.dim_customer,
    sku         TEXT REFERENCES consumption.dim_product,
    date_id     DATE REFERENCES consumption.dim_date,
    quantity    INTEGER,
    unit_price  NUMERIC(12,2),
    revenue     NUMERIC(14,2),
    cost        NUMERIC(14,2),
    margin      NUMERIC(14,2),
    channel     TEXT
);
"""

CONSUMPTION_LOAD = """
INSERT INTO consumption.dim_customer SELECT * FROM staging.customers;
INSERT INTO consumption.dim_product   SELECT * FROM staging.products;
INSERT INTO consumption.fact_orders (
    order_id, customer_id, sku, date_id,
    quantity, unit_price, revenue, cost, margin, channel
)
SELECT
    o.order_id, o.customer_id, o.sku, o.order_date,
    o.quantity, o.unit_price, o.revenue,
    p.cost * o.quantity,
    o.revenue - (p.cost * o.quantity),
    o.channel
FROM staging.orders o
JOIN staging.products  p ON o.sku         = p.sku
JOIN staging.customers c ON o.customer_id = c.customer_id;
"""

def build_dim_date(conn):
    cur = conn.cursor()
    cur.execute("SELECT MIN(order_date), MAX(order_date) FROM staging.orders")
    min_d, max_d = cur.fetchone()
    dates = pd.date_range(min_d, max_d, freq="D")
    rows = [
        (d.date(), d.year, d.quarter, d.month,
         d.strftime("%B"), d.isocalendar()[1], d.day, d.strftime("%A"))
        for d in dates
    ]
    execute_values(
        cur,
        "INSERT INTO consumption.dim_date (date_id,year,quarter,month,month_name,week,day,weekday) VALUES %s",
        rows,
    )
    conn.commit()
    cur.close()

def load_consumption(conn):
    cur = conn.cursor()
    cur.execute(CONSUMPTION_DDL)
    conn.commit()
    build_dim_date(conn)
    cur.execute(CONSUMPTION_LOAD)
    conn.commit()
    cur.close()


def run():
    conn = get_conn()
    load_raw(conn)
    load_staging(conn)
    load_consumption(conn)
    conn.close()
    print("ETL completado")

if __name__ == "__main__":
    run()
