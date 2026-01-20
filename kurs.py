import os
import sys
import time
import requests
from datetime import datetime
from pymongo import MongoClient
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed

# === Настройка окружения для PySpark ===
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# === ID арктических регионов ===
ARCTIC_AREA_IDS = [1061, 1414, 1985, 1982, 1174, 1146, 1008, 1077]
MAX_WORKERS = 8

# === 1. Сбор данных по одному региону (без ограничения по дате) ===
def fetch_vacancies_for_area(area_id):
    client = MongoClient("mongodb://localhost:27017/")
    db = client["arctic_labor"]
    raw_collection = db["raw_vacancies"]
    new_count = 0

    for page in range(0, 20):  # hh.ru ограничивает пагинацию ~2000 записями (20 стр. × 100)
        try:
            resp = requests.get(
                "https://api.hh.ru/vacancies",
                params={
                    'area': area_id,
                    'per_page': 100,
                    'page': page
                },
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get('items', [])
            if not items:
                break

            new_items = []
            for item in items:
                if item.get("archived", False):
                    continue
                item["_fetched_at"] = datetime.utcnow()
                item["_source_region_id"] = area_id
                new_items.append(item)

            if new_items:
                try:
                    raw_collection.insert_many(new_items, ordered=False)
                    new_count += len(new_items)
                    print(f"+{len(new_items)} из региона {area_id}, стр. {page}")
                except Exception as e:
                    if "duplicate key" not in str(e):
                        print(f"Ошибка вставки региона {area_id}: {e}")

            if page >= data.get('pages', 1) - 1:
                break
            time.sleep(0.2)

        except Exception as e:
            print(f"Ошибка региона {area_id}, стр. {page}: {e}")
            break

    return new_count

# === 1. Параллельный сбор с полной очисткой (без days_back) ===
def fetch_and_store_raw_to_mongo():
    client = MongoClient("mongodb://localhost:27017/")
    db = client["arctic_labor"]
    raw_collection = db["raw_vacancies"]

    # УДАЛЯЕМ ВСЕ СТАРЫЕ ДАННЫЕ
    raw_collection.delete_many({})
    print("Все старые данные удалены.")

    try:
        raw_collection.create_index("id", unique=True)
    except Exception as e:
        if "already exists" not in str(e):
            print(f"Индекс не создан: {e}")

    print("Запуск параллельного сбора данных...")
    total_new = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_area = {
            executor.submit(fetch_vacancies_for_area, area_id): area_id
            for area_id in ARCTIC_AREA_IDS
        }

        for future in as_completed(future_to_area):
            try:
                count = future.result()
                total_new += count
            except Exception as e:
                area_id = future_to_area[future]
                print(f"Исключение в регионе {area_id}: {e}")

    print(f"Всего вакансий сохранено: {total_new}")
    return total_new

# === 2. Обработка: PySpark → Parquet (для Superset) ===
def process_with_spark_for_superset():
    from pymongo import MongoClient
    import pandas as pd

    spark = SparkSession.builder \
        .appName("ArcticLaborMarket") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    client = MongoClient("mongodb://127.0.0.1:27017/")
    db = client["arctic_labor"]
    raw_data = list(db.raw_vacancies.find())

    if not raw_data:
        print("Нет данных в MongoDB.")
        spark.stop()
        return None

    # Удаляем _id
    for doc in raw_data:
        doc.pop('_id', None)

    # === Вручную формируем чистые записи ===
    clean_records = []
    for doc in raw_data:
        salary_avg = None
        sal = doc.get("salary")
        if isinstance(sal, dict) and sal.get("currency") == "RUR":
            s_from = sal.get("from")
            s_to = sal.get("to")
            if s_from is not None and s_to is not None:
                salary_avg = float((s_from + s_to) / 2)
            elif s_from is not None:
                salary_avg = float(s_from)
            elif s_to is not None:
                salary_avg = float(s_to)

        profession = None
        roles = doc.get("professional_roles")
        if isinstance(roles, list) and len(roles) > 0:
            first_role = roles[0]
            if isinstance(first_role, dict):
                profession = first_role.get("name")

        record = {
            "vacancy_id": str(doc.get("id", "")),
            "title": doc.get("name", ""),
            "region": doc.get("area", {}).get("name", ""),
            "published_at": doc.get("published_at", ""),
            "salary_avg": salary_avg,
            "experience": doc.get("experience", {}).get("name", ""),
            "employment_type": doc.get("employment", {}).get("name", ""),
            "schedule": doc.get("schedule", {}).get("name", ""),
            "profession": profession
        }
        clean_records.append(record)

    # Создаём Spark DataFrame
    df_spark = spark.createDataFrame(clean_records)

    # === Конвертируем в Pandas и сохраняем через pyarrow ===
    df_pandas = df_spark.toPandas()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    parquet_path = os.path.join(base_dir, "data", "superset", "arctic_vacancies.parquet")

    print(f"Попытка сохранить файл: {parquet_path}")
    os.makedirs(os.path.dirname(parquet_path), exist_ok=True)

    df_pandas.to_parquet(parquet_path, index=False, engine="pyarrow")
    print("Файл успешно сохранён")

    spark.stop()
    return parquet_path


# === Запуск ===
if __name__ == "__main__":
    print("Полная перепарсивка данных (все вакансии, без ограничения по дате)...")
    fetch_and_store_raw_to_mongo()
    parquet_file = process_with_spark_for_superset()
    print("Конвейер завершён.")