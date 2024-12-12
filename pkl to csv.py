import pandas as pd
import os

def convert_pkl_to_csv(pkl_file_path):
    # Определяем путь для сохранения CSV
    csv_file_path = os.path.splitext(pkl_file_path)[0] + '.csv'
    
    try:
        # Попытка считать данные из pkl как DataFrame
        df = pd.read_pickle(pkl_file_path)
        
        # Проверяем тип данных
        if isinstance(df, pd.DataFrame):
            print("Данные из pkl файла (DataFrame):")
            print(df.head())  # Вывод первых строк для проверки
            
            # Сохранение данных в CSV
            df.to_csv(csv_file_path, index=False, encoding='utf-8', sep=';')
            print(f"Файл {csv_file_path} успешно создан.")
        elif isinstance(df, list):
            print("Данные из pkl файла (список):")
            print(df[:5])  # Вывод первых 5 элементов для проверки
            
            # Преобразуем список в DataFrame и сохраняем
            df = pd.DataFrame(df)
            df.to_csv(csv_file_path, index=False, encoding='utf-8', sep=';')
            print(f"Файл {csv_file_path} успешно создан.")
        else:
            print(f"Неизвестный формат данных: {type(df)}")
    except Exception as e:
        print(f"Ошибка при чтении pkl файла: {e}")

# Укажите путь к вашему pkl файлу
pkl_file_path = r"C:\Users\klimo\Desktop\Ycheba\engineering\prac 4\subitem.pkl"
convert_pkl_to_csv(pkl_file_path)
