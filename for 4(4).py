import pandas as pd
import json

# Чтение данных о товарах из JSON файла с помощью pandas
def parse_data_from_json(data):
    return pd.read_json(data)

# Загрузка и обработка данных из файла _update_data.text
def load_updates_data(file_path):
    updates = []
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_text_data = f.read()
    
    for block in raw_text_data.split('====='):
        record = {}
        for line in block.strip().split('\n'):
            if '::' in line:
                key, value = line.split('::', 1)
                record[key.strip()] = value.strip()
        if record:
            updates.append(record)
    
    return pd.DataFrame(updates)

# Применение обновлений к данным
def apply_updates(products, updates):
    # Добавляем колонку для счетчика обновлений
    products['update_count'] = 0
    
    for _, update in updates.iterrows():
        product_name = update['name']
        method = update['method']
        param = update['param']
        
        if method == 'quantity_sub':
            products.loc[products['name'] == product_name, 'quantity'] += int(param)
            products.loc[products['name'] == product_name, 'update_count'] += 1
        elif method == 'price_percent':
            products.loc[products['name'] == product_name, 'price'] *= (1 + float(param))
            products.loc[products['name'] == product_name, 'update_count'] += 1
        elif method == 'available':
            products.loc[products['name'] == product_name, 'isAvailable'] = param.lower() == 'true'
            products.loc[products['name'] == product_name, 'update_count'] += 1
        elif method == 'remove':
            products = products[products['name'] != product_name]
    
    return products

# Топ-10 самых обновляемых товаров
def get_top_updated_products(products, limit=10):
    return products[['name', 'update_count']].sort_values(by='update_count', ascending=False).head(limit)

# Статистика по цене товаров
def get_price_stats(products):
    return products.groupby('category')['price'].agg(['sum', 'min', 'max', 'mean', 'count'])

# Статистика по остаткам товаров
def get_quantity_stats(products):
    return products.groupby('category')['quantity'].agg(['sum', 'min', 'max', 'mean', 'count'])

# Произвольный запрос (фильтрация по цене и количество > 100)
def arbitrary_query(products):
    return products[(products['price'] > 1000) & (products['quantity'] > 100)]

# Основная функция
def main():
    # Загрузка данных
    products_file = '_product_data.json'
    updates_file = '_update_data.text'

    # Преобразуем JSON файл с товарами в pandas DataFrame
    products = parse_data_from_json(products_file)

    # Преобразуем текстовый файл с обновлениями в pandas DataFrame
    updates = load_updates_data(updates_file)

    # Применяем обновления к данным
    updated_products = apply_updates(products, updates)

    # 1. Топ-10 самых обновляемых товаров
    top_updated = get_top_updated_products(updated_products)
    print("Top Updated Products:")
    print(top_updated)
    top_updated.to_json('4_4_1.json', orient='records', lines=True)

    # 2. Статистика по цене товаров
    price_stats = get_price_stats(updated_products)
    print("\nPrice Stats:")
    print(price_stats)
    price_stats.to_json('4_4_2.json', orient='table')

    # 3. Статистика по остаткам товаров
    quantity_stats = get_quantity_stats(updated_products)
    print("\nQuantity Stats:")
    print(quantity_stats)
    quantity_stats.to_json('4_4_3.json', orient='table')

    # 4. Произвольный запрос
    arbitrary_result = arbitrary_query(updated_products)
    print("\nArbitrary Query Result:")
    print(arbitrary_result)
    arbitrary_result.to_json('4_4_4.json', orient='records', lines=True)

if __name__ == '__main__':
    main()
