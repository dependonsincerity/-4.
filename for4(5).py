import pandas as pd
import sqlite3
import json

# Чтение данных из CSV файлов
def parse_data_from_csv(file_path):
    return pd.read_csv(file_path)

# Создание базы данных и таблиц
def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS electronic (
        patientunitstayid INTEGER PRIMARY KEY,
        patienthealthsystemstayid INTEGER,
        gender TEXT,
        age INTEGER,
        ethnicity TEXT,
        hospitalid INTEGER,
        wardid INTEGER,
        apacheadmissiondx TEXT,
        admissionheight REAL,
        hospitaladmittime24 TEXT,
        hospitaladmitoffset INTEGER,
        hospitaladmitsource TEXT,
        hospitaldischargeyear INTEGER,
        hospitaldischargetime24 TEXT,
        hospitaldischargeoffset INTEGER,
        hospitaldischargelocation TEXT,
        hospitaldischargestatus TEXT,
        unittype TEXT,
        unitadmittime24 TEXT,
        unitadmitsource TEXT,
        unitvisitnumber INTEGER,
        unitstaytype TEXT,
        admissionweight REAL,
        dischargeweight REAL,
        unitdischargetime24 TEXT,
        unitdischargeoffset INTEGER,
        unitdischargelocation TEXT,
        unitdischargestatus TEXT,
        uniquepid TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fashion_products (
        user_id INTEGER PRIMARY KEY,
        product_id INTEGER,
        product_name TEXT,
        brand TEXT,
        category TEXT,
        price REAL,
        rating REAL,
        color TEXT,
        size TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        id INTEGER PRIMARY KEY,
        url TEXT,
        name TEXT,
        sku TEXT,
        price REAL,
        currency TEXT,
        primary_category TEXT,
        reviewer_name TEXT,
        review_title TEXT,
        description TEXT,
        rating REAL,
        published_at TEXT,
        scraped_at TEXT
    )
    """)
    conn.commit()

# Загрузка данных в таблицы
def load_data_to_db(conn, electronic_data, fashion_data, reviews_data):
    cursor = conn.cursor()

    # Загрузка данных в таблицу electronic
    electronic_data.to_sql('electronic', conn, if_exists='replace', index=False)

    # Загрузка данных в таблицу fashion_products
    fashion_data.to_sql('fashion_products', conn, if_exists='replace', index=False)

    # Загрузка данных в таблицу reviews
    reviews_data.to_sql('reviews', conn, if_exists='replace', index=False)

    conn.commit()

# Пример запроса 1: выборка с простым условием, сортировка и ограничение
def query_1(conn):
    query = """
    SELECT * FROM fashion_products
    WHERE price > 50
    ORDER BY price DESC
    LIMIT 10
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_1.json', orient='records', lines=True)

# Пример запроса 2: подсчет объектов по условию (по категориям)
def query_2(conn):
    query = """
    SELECT category, COUNT(*) as count
    FROM fashion_products
    GROUP BY category
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_2.json', orient='records', lines=True)

# Пример запроса 3: агрегация по цене (сумма, минимум, максимум, среднее)
def query_3(conn):
    query = """
    SELECT category, 
           SUM(price) as total_price,
           MIN(price) as min_price,
           MAX(price) as max_price,
           AVG(price) as avg_price
    FROM fashion_products
    GROUP BY category
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_3.json', orient='records', lines=True)

# Пример запроса 4: Обновление данных (например, увеличение цены)
def query_4(conn):
    cursor = conn.cursor()
    cursor.execute("""
    UPDATE fashion_products
    SET price = price * 1.1
    WHERE rating > 4.0
    """)
    conn.commit()

# Пример запроса 5: Статистика по возрасту пациентов в таблице electronic
def query_5(conn):
    query = """
    SELECT 
        gender,
        AVG(age) as avg_age,
        MIN(age) as min_age,
        MAX(age) as max_age,
        COUNT(*) as patient_count
    FROM electronic
    GROUP BY gender
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_5.json', orient='records', lines=True)

# Пример запроса 6: Отбор товаров с отзывами с рейтингом выше 4.0
def query_6(conn):
    query = """
    SELECT * 
    FROM reviews
    WHERE rating > 4.0
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_6.json', orient='records', lines=True)

# Пример произвольного запроса: Строки, где цена больше 30 и рейтинг выше 3
def arbitrary_query(conn):
    query = """
    SELECT * FROM fashion_products
    WHERE price > 30 AND rating > 3
    """
    result = pd.read_sql(query, conn)
    result.to_json('4_5_4.json', orient='records', lines=True)

# Основная функция
def main():
    # Загрузка данных из CSV файлов
    electronic_file = 'electronic.csv'
    fashion_file = 'fashion_products.csv'
    reviews_file = 'reviews.csv'

    electronic_data = parse_data_from_csv(electronic_file)
    fashion_data = parse_data_from_csv(fashion_file)
    reviews_data = parse_data_from_csv(reviews_file)

    # Создание базы данных и таблиц
    db_path = 'product_database.db'
    conn = sqlite3.connect(db_path)

    create_tables(conn)

    # Загрузка данных в базу данных
    load_data_to_db(conn, electronic_data, fashion_data, reviews_data)

    # Выполнение запросов
    query_1(conn)
    query_2(conn)
    query_3(conn)
    query_4(conn)
    query_5(conn)
    query_6(conn)
    arbitrary_query(conn)

    # Закрытие соединения
    conn.close()

if __name__ == '__main__':
    main()
