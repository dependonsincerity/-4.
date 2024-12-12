import pandas as pd
import sqlite3
import json
import os

# Загрузка данных из файла _part_1.csv
file_path_csv = r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\_part_1.csv'
data_csv = pd.read_csv(file_path_csv, sep=';', skip_blank_lines=True)
data_csv_cleaned = data_csv.dropna(how='all')

# Загрузка данных из файла _part_2.text с использованием кодировки utf-8
file_path_text = r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\_part_2.text'
with open(file_path_text, 'r', encoding='utf-8', errors='ignore') as f:  # Указываем 'utf-8', игнорируя ошибки
    raw_text_data = f.read()

# Преобразуем данные из _part_2.text в структуру
records = []
for block in raw_text_data.split('====='):
    record = {}
    for line in block.strip().split('\n'):
        if '::' in line:
            key, value = line.split('::', 1)
            record[key.strip()] = value.strip()
    if record:
        records.append(record)

# Преобразуем в DataFrame
data_text = pd.DataFrame(records)

# Преобразование типов данных в текстовом файле
data_text['duration_ms'] = pd.to_numeric(data_text['duration_ms'], errors='coerce')
data_text['year'] = pd.to_numeric(data_text['year'], errors='coerce')
data_text['tempo'] = pd.to_numeric(data_text['tempo'], errors='coerce')
data_text['loudness'] = pd.to_numeric(data_text['loudness'], errors='coerce')

# Объединение данных
columns_to_merge = ['artist', 'song', 'duration_ms', 'year', 'tempo', 'genre', 'loudness', 'instrumentalness', 'explicit']
data_csv_cleaned['instrumentalness'] = None
data_csv_cleaned['explicit'] = None
merged_data = pd.concat([data_csv_cleaned[columns_to_merge], data_text[columns_to_merge]], ignore_index=True)

# Создание базы данных SQLite и запись объединенных данных
db_path = r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\music_data.db'
conn = sqlite3.connect(db_path)
merged_data.to_sql('music_data', conn, if_exists='replace', index=False)

# Запрос 1: Вывод первых (VAR+10) строк, отсортированных по duration_ms, в файл JSON
VAR = 5
sorted_data = merged_data.sort_values(by='duration_ms').head(VAR + 10)
sorted_data.to_json(r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\4_3_1.json', orient='records', lines=True)

# Запрос 2: Вывод суммы, мин, макс, среднего для поля duration_ms
stats = merged_data['duration_ms'].agg(['sum', 'min', 'max', 'mean'])
with open(r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\4_3_2.json', 'w') as f:
    f.write(stats.to_json())

# Запрос 3: Вывод частоты встречаемости для категориального поля genre
frequency = merged_data['genre'].value_counts()
with open(r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\4_3_3.json', 'w') as f:
    f.write(frequency.to_json())

# Запрос 4: Фильтрация строк по предикату (например, год > 2010), сортировка по tempo, сохранение первых (VAR+15) строк в JSON
filtered_sorted_data = merged_data[merged_data['year'] > 2010].sort_values(by='tempo').head(VAR + 15)
filtered_sorted_data.to_json(r'C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\4_3_4.json', orient='records', lines=True)

# Закрытие соединения с базой данных
conn.close()
