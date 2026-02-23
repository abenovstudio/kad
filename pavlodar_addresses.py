"""
ПАРСЕР АДРЕСНОГО РЕЕСТРА — ПАВЛОДАР
Датасеты:
  s_ats — здания/дома (первичные объекты)
  s_pb  — квартиры (вторичные объекты)

Запуск: python pavlodar_addresses.py
"""

import requests
import json
import time
import csv
import os

BASE    = "https://data.egov.kz"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://data.egov.kz/datasets/view?index=s_ats",
}

BATCH   = 500
DELAY   = 0.4
REGION  = "ПАВЛОДАР"

# ============================================================
# ШАГ 1: Смотрим поля датасетов
# ============================================================

def get_mapping(index):
    url = f"{BASE}/api/v4/mapping/{index}/v1"
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code == 200:
        return r.json()
    return None

def get_sample(index, query=None):
    q = query or {"size": 3}
    url = f"{BASE}/api/detailed/{index}/v1?source=" + json.dumps(q, ensure_ascii=False)
    r = requests.get(url, headers=HEADERS, timeout=15)
    if r.status_code == 200:
        data = r.json()
        if isinstance(data, dict):
            return data.get("totalCount", 0), data.get("data", [])
    return 0, []

# ============================================================
# ШАГ 2: Загрузка всех домов Павлодара из s_ats
# ============================================================

def load_buildings(output_file="pavlodar_buildings.csv"):
    print("\n🏠 Загружаем здания (s_ats)...")

    # Сначала смотрим поля
    print("   Проверяем поля...")
    total, sample = get_sample("s_ats", {"size": 2, "query": {"match": {"_all": REGION}}})
    
    if not sample:
        # Пробуем без фильтра
        total, sample = get_sample("s_ats", {"size": 2})
    
    if sample:
        print(f"   Поля: {list(sample[0].keys())}")
        print(f"   Пример: {json.dumps(sample[0], ensure_ascii=False)[:300]}")
    else:
        print("   ❌ Нет данных")
        return []

    # Определяем поле для фильтра по региону
    first = sample[0]
    region_field = None
    for field in first.keys():
        val = str(first[field]).upper()
        if "ПАВЛОДАР" in val or "OBLAST" in val.upper() or "ОБЛА" in val:
            region_field = field
            break

    print(f"   Поле региона: {region_field}")

    # Загружаем все здания Павлодара
    all_records = []
    offset = 0
    
    while True:
        if region_field:
            query = {
                "from": offset,
                "size": BATCH,
                "query": {"match": {region_field: REGION}}
            }
        else:
            query = {"from": offset, "size": BATCH}

        url = f"{BASE}/api/detailed/s_ats/v1?source=" + json.dumps(query, ensure_ascii=False)
        r = requests.get(url, headers=HEADERS, timeout=20)
        
        if r.status_code != 200:
            print(f"   ❌ HTTP {r.status_code}")
            break
        
        data = r.json()
        batch = data.get("data", []) if isinstance(data, dict) else data
        
        if not batch:
            break
        
        # Фильтруем по Павлодару если нет серверного фильтра
        if not region_field:
            batch = [rec for rec in batch 
                    if REGION in json.dumps(rec, ensure_ascii=False).upper()]
        
        all_records.extend(batch)
        offset += len(batch)
        
        total_count = data.get("totalCount", "?") if isinstance(data, dict) else "?"
        print(f"   Загружено: {len(all_records)} (всего в базе: {total_count})")
        
        if len(batch) < BATCH:
            break
            
        time.sleep(DELAY)

    print(f"   ✅ Зданий Павлодара: {len(all_records)}")

    # Сохраняем
    if all_records:
        keys = list(all_records[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(all_records)
        print(f"   💾 {output_file}")

    return all_records

# ============================================================
# ШАГ 3: Загрузка квартир из s_pb
# ============================================================

def load_apartments(buildings, output_file="pavlodar_apartments.csv"):
    print("\n🏘️  Загружаем квартиры (s_pb)...")

    # Смотрим структуру
    total, sample = get_sample("s_pb", {"size": 3})
    
    if not sample:
        print("   ❌ Нет данных в s_pb")
        return []
    
    print(f"   Поля: {list(sample[0].keys())}")
    print(f"   Пример: {json.dumps(sample[0], ensure_ascii=False)[:300]}")
    print(f"   Всего в базе: {total:,}")

    # s_pb — это квартиры привязанные к зданиям через ID
    # Загружаем по Павлодару
    all_apts = []
    offset = 0

    while True:
        query = {
            "from": offset,
            "size": BATCH,
            "query": {"match": {"_all": REGION}}
        }

        url = f"{BASE}/api/detailed/s_pb/v1?source=" + json.dumps(query, ensure_ascii=False)
        r = requests.get(url, headers=HEADERS, timeout=20)
        
        if r.status_code != 200:
            print(f"   ❌ HTTP {r.status_code}")
            break

        data = r.json()
        batch = data.get("data", []) if isinstance(data, dict) else data
        
        if not batch:
            break

        all_apts.extend(batch)
        offset += len(batch)
        total_count = data.get("totalCount", "?") if isinstance(data, dict) else "?"
        print(f"   Загружено квартир: {len(all_apts)} (всего: {total_count})")

        if len(batch) < BATCH:
            break

        time.sleep(DELAY)

    print(f"   ✅ Квартир Павлодара: {len(all_apts)}")

    if all_apts:
        keys = list(all_apts[0].keys())
        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=keys)
            w.writeheader()
            w.writerows(all_apts)
        print(f"   💾 {output_file}")

    return all_apts

# ============================================================
# ГЛАВНАЯ
# ============================================================

def main():
    print("=" * 60)
    print("АДРЕСНЫЙ РЕЕСТР ПАВЛОДАРА")
    print("=" * 60)

    # Шаг 1: Проверяем оба датасета
    print("\n🔍 Шаг 1: Проверка датасетов")
    for index in ["s_ats", "s_pb", "s_street", "rka", "address_register"]:
        total, sample = get_sample(index, {"size": 1})
        if sample:
            print(f"   ✅ {index}: {total:,} записей | поля: {list(sample[0].keys())[:8]}")
        else:
            print(f"   ❌ {index}: не найден")
        time.sleep(0.3)

    # Шаг 2: Загружаем здания
    buildings = load_buildings("pavlodar_buildings.csv")

    # Шаг 3: Загружаем квартиры
    apartments = load_apartments(buildings, "pavlodar_apartments.csv")

    print(f"\n{'='*60}")
    print(f"ИТОГ:")
    print(f"  Зданий:   {len(buildings):,}")
    print(f"  Квартир:  {len(apartments):,}")
    print(f"  Файлы:    pavlodar_buildings.csv, pavlodar_apartments.csv")

if __name__ == "__main__":
    main()
