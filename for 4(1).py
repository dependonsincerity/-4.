import pandas as pd
import sqlite3
import json
import os

# Парсинг
def parse_data_from_csv(data):
    df = pd.read_csv(data, delimiter=';') 
    
    # первые строчки
    print("\nПервые строки файла:")
    print(df.head())  

    # Убираем строки, где есть только пустые значения в каждом столбце
    df = df.dropna(how='all')

    # Преобразуем данные в числовой формат
    df['prob_price'] = pd.to_numeric(df['prob_price'], errors='coerce')
    df['views'] = pd.to_numeric(df['views'], errors='coerce')
    df['floors'] = pd.to_numeric(df['floors'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')

    
    df.fillna(0, inplace=True)

    return df

# Подключаемся к SQL
def connect_to_db(db_file):
    connection = sqlite3.connect(db_file)
    connection.row_factory = sqlite3.Row  
    return connection

# Записываем данные в базу данных
def insert_data(db, data):
    cursor = db.cursor()
    data.to_sql('products', db, if_exists='replace', index=False)
    db.commit()

# Вывод первых (VAR+10) отсортированных по числовому полю строк из таблицы в файл формата json
def get_top_by_views(db, limit):
    cursor = db.cursor()
    result = cursor.execute("SELECT * FROM products ORDER BY views DESC LIMIT ?", [limit])
    items = [dict(row) for row in result.fetchall()]
    cursor.close()
    return items

# Вывод суммы, минимального, максимального, среднего по произвольному числовому полю
def min_max(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
            SUM(prob_price) as sum_prob_price,
            AVG(prob_price) as avg_prob_price,
            MIN(prob_price) as min_prob_price,
            MAX(prob_price) as max_prob_price
        FROM products
    """)
    items = dict(result.fetchone())
    with open(f'4_1_1.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))
    cursor.close()

# Вывод частоты встречаемости для категориального поля
def get_occurrence(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
            city, COUNT(*) as frequency
        FROM products
        GROUP BY city
        ORDER BY frequency DESC
    """)
    items = [dict(row) for row in result.fetchall()]
    with open(f'4_1_2.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))
    cursor.close()

# Вывод первых (VAR+10) отфильтрованных по произвольному предикату отсортированных по числовому полю строк из таблицы в файл формата json
def get_filtered_sorted_by_price(db, min_price, limit):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT * 
        FROM products
        WHERE prob_price > ?
        ORDER BY prob_price DESC 
        LIMIT ?
    """, [min_price, limit])
    items = [dict(row) for row in result.fetchall()]
    cursor.close()
    return items

# Параметры для работы
input_file = 'item.csv'  # Путь к CSV файлу
db_file = 'products.db'  # Название базы данных
output_folder = '.'  # Место сохранения результатов

# Считываем данные из CSV
items = parse_data_from_csv(input_file)

# Подключаемся к базе данных
db = connect_to_db(db_file)

# Создаём таблицу и загружаем данные в SQLite
cursor = db.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT,
        street TEXT,
        city TEXT,
        zipcode TEXT,
        floors INTEGER,
        year INTEGER,
        parking TEXT,
        prob_price REAL,
        views INTEGER
    )
""")
db.commit()

# Заполняем таблицу данными
insert_data(db, items)

# 1. ПЕРВЫЕ VAR+10 строк, отсортированных по числовому полю 'views'
top_items = get_top_by_views(db, 17)  # VAR = 7
with open(f"{output_folder}/4_1_3.json", 'w', encoding='utf-8') as file:
    file.write(json.dumps(top_items, ensure_ascii=False, indent=4))

# 2. СУММА, МИН, МАКС, СРЕДНЕЕ по числовому полю 'prob_price'
min_max(db)

# 3. Частота встречаемости по полю 'city'
get_occurrence(db)

# 4. ПЕРВЫЕ VAR+10 отфильтрованных по цене и отсортированных по числовому полю 'prob_price'
filtered_items = get_filtered_sorted_by_price(db, 1000, 17)  # Пример фильтрации по цене > 1000
with open(f"{output_folder}/4_1_4.json", 'w', encoding='utf-8') as file:
    file.write(json.dumps(filtered_items, ensure_ascii=False, indent=4))

# Закрытие соединения с базой данных
db.close()
