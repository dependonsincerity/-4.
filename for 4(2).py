import pandas as pd
import sqlite3
import json

# Считываем данные с CSV файла, созданного из pkl
def parse_data_from_csv(data):
    df = pd.read_csv(data, delimiter=';')
    print("Информация о данных:")
    print(df.info()) 
    print("\nПервые строки файла:")
    print(df.head())  
    return df

# Подключаемся к базе данных SQLite
def connect_to_db(db_file):
    connection = sqlite3.connect(db_file)
    connection.row_factory = sqlite3.Row
    return connection

# Записываем данные в новую таблицу
def insert_data_to_second_table(db, data):
    cursor = db.cursor()
    data.to_sql('reviews', db, if_exists='replace', index=False)
    db.commit()

# Выполняем запросы с использованием связи между таблицами
def perform_queries(db):
    cursor = db.cursor()

    # 1. Выводим список продуктов и их средний рейтинг (из таблицы reviews)
    q1 = """
        SELECT p.name, AVG(r.rating) as avg_rating
        FROM products p
        JOIN reviews r ON p.name = r.name
        GROUP BY p.name
        ORDER BY avg_rating DESC
        LIMIT 10
    """
    result1 = cursor.execute(q1).fetchall()
    print("\nТоп-10 продуктов по среднему рейтингу:")
    print(result1)

    # 2. Выводим список продуктов с удобством использования (из reviews) выше 4.5
    q2 = """
        SELECT p.name, r.convenience, r.comment
        FROM products p
        JOIN reviews r ON p.name = r.name
        WHERE r.convenience > 4.5
        ORDER BY r.convenience DESC
    """
    result2 = cursor.execute(q2).fetchall()
    print("\nПродукты с удобством использования выше 4.5:")
    print(result2)

    # 3. Выводим количество отзывов для каждого города (связь через таблицу products)
    q3 = """
        SELECT p.city, COUNT(r.name) as review_count
        FROM products p
        JOIN reviews r ON p.name = r.name
        GROUP BY p.city
        ORDER BY review_count DESC
    """
    result3 = cursor.execute(q3).fetchall()
    print("\nКоличество отзывов для каждого города:")
    print(result3)

    # Сохранение результатов в JSON
    with open("4_2_1.json", "w", encoding="utf-8") as file:
        json.dump(
            {"q1": [dict(row) for row in result1],
             "q2": [dict(row) for row in result2],
             "q3": [dict(row) for row in result3]},
            file, ensure_ascii=False, indent=4
        )

# Параметры для работы
subitem_csv = "subitem.csv"  # CSV файл, созданный из pkl
db_file = "products.db"  # Используем ту же базу данных

# Считываем данные из CSV
reviews_df = parse_data_from_csv(subitem_csv)

# Подключаемся к базе данных
db = connect_to_db(db_file)

# Создаем новую таблицу reviews и загружаем данные
cursor = db.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS reviews (
        name TEXT,
        rating REAL,
        convenience REAL,
        security REAL,
        functionality REAL,
        comment TEXT
    )
""")
db.commit()

# Заполняем таблицу данными
insert_data_to_second_table(db, reviews_df)

# Выполняем запросы
perform_queries(db)

# Закрытие соединения с базой данных
db.close()
