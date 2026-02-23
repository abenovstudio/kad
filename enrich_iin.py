"""
ОБОГАЩЕНИЕ ИИН из ГБД ЮЛ (data.egov.kz)
 
Логика:
  1. ИП: nameru содержит только ФИО (без ТОО/АО/ОО) → bin = ИИН физлица
  2. Директора: поле director → ФИО → ищем совпадение в нашем списке жителей

Запуск:
  pip install requests pandas
  python enrich_iin.py
"""

import requests
import json
import time
import csv

# ============================================================
# НАСТРОЙКИ
# ============================================================
RESIDENTS_FILE = "vdg-pvl-list.csv"   # ваш список жителей
OUTPUT_FILE    = "residents_with_iin.csv"
DELAY          = 0.4                   # секунд между запросами

BASE = "https://data.egov.kz"

# Заголовки как у браузера — иначе блокирует
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
    "Referer": "https://data.egov.kz/datasets/view?index=gbd_ul",
    "Origin": "https://data.egov.kz",
}

# Стоп-слова: если nameru содержит любое из них — это не ИП
NOT_IP = [
    "ТОО", "АО", "ОАО", "ЗАО", "ОО", "НАО", "КГП", "ГКП", "РГП",
    "ГККП", "КП", "РГКП", "КГКП", "ФЛ", "ПК", "СПК", "МКО", "ФОНД",
    "UNION", "ОБЪЕДИНЕНИЕ", "АССОЦИАЦИЯ", "СОЮЗ", "КООПЕРАТИВ",
    "ЦЕНТР", "ШКОЛА", "БОЛЬНИЦА", "ИНСТИТУТ", "УНИВЕРСИТЕТ",
    "АКАДЕМИЯ", "КОЛЛЕДЖ", "УПРАВЛЕНИЕ", "ДЕПАРТАМЕНТ",
    "«", '"', "LIMITED", "LLP", "LLC",
]

def is_ip(nameru):
    """Проверяет — это ИП (физлицо) или юрлицо"""
    if not nameru:
        return False
    n = str(nameru).upper()
    return not any(word in n for word in NOT_IP)

def fetch_by_director(fio, size=5):
    """Ищет записи по полю director"""
    query = {
        "size": size,
        "query": {
            "match": {"director": fio}
        }
    }
    try:
        url = f"{BASE}/api/detailed/gbd_ul/v1?source=" + json.dumps(query, ensure_ascii=False)
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("data", [])
    except Exception as e:
        pass
    return []

def fetch_ip_by_name(fio, size=5):
    """Ищет ИП по полному имени в nameru"""
    query = {
        "size": size,
        "query": {
            "match": {"nameru": fio}
        }
    }
    try:
        url = f"{BASE}/api/detailed/gbd_ul/v1?source=" + json.dumps(query, ensure_ascii=False)
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return [r for r in data.get("data", []) if is_ip(r.get("nameru", ""))]
    except:
        pass
    return []

def build_fio(row):
    """Собирает ФИО из колонок last_name, first_name, patronymic"""
    parts = []
    for col in ["last_name", "first_name", "patronymic"]:
        v = str(row.get(col, "")).strip()
        if v and v.lower() != "nan":
            parts.append(v.upper())
    return " ".join(parts)

def read_rows(file_path):
    for enc in ["utf-8", "utf-8-sig", "cp1251"]:
        try:
            with open(file_path, "r", encoding=enc, newline="") as f:
                reader = csv.DictReader(f)
                columns = list(reader.fieldnames or [])
                rows = list(reader)
            return rows, columns, enc
        except Exception:
            continue
    return None, None, None

def write_rows(file_path, rows, columns):
    with open(file_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

def main():
    # Загружаем список жителей
    print(f"📂 Читаем {RESIDENTS_FILE}...")
    rows, columns, enc = read_rows(RESIDENTS_FILE)
    
    if rows is None:
        print("❌ Файл не найден")
        return

    print(f"   ✅ {len(rows):,} записей, кодировка {enc}")
    print(f"   Колонки: {columns}")

    # Добавляем колонки для результата
    if "iin" not in columns:
        columns.append("iin")
    if "iin_source" not in columns:
        columns.append("iin_source")
    for row in rows:
        row.setdefault("iin", "")
        row.setdefault("iin_source", "")

    # Фильтр только Павлодар
    if "place" in columns:
        work_indices = [
            i for i, row in enumerate(rows)
            if "ПАВЛОДАР" in str(row.get("place", "")).upper()
        ]
        print(f"   🏙️  Павлодар: {len(work_indices):,} записей")
    else:
        work_indices = list(range(len(rows)))

    # Убираем уже обработанных
    todo_indices = [
        idx for idx in work_indices
        if str(rows[idx].get("iin", "")).strip() == ""
    ]
    print(f"   ⏳ Осталось обработать: {len(todo_indices):,}")

    found_count = 0
    processed = 0

    for idx in todo_indices:
        row = rows[idx]
        fio = build_fio(row)
        if not fio:
            continue

        processed += 1
        if processed % 100 == 0:
            print(f"   [{processed}/{len(todo_indices)}] найдено ИИН: {found_count} | последний: {fio}")
            # Сохраняем промежуточный результат
            write_rows(OUTPUT_FILE, rows, columns)

        iin = None
        source = None

        # Способ 1: ищем как ИП (nameru = ФИО)
        ip_records = fetch_ip_by_name(fio)
        if ip_records:
            rec = ip_records[0]
            bin_val = rec.get("bin", "")
            # Проверяем что первые 6 цифр БИН совпадают с датой рождения
            dob = str(row.get("date_birth", "")).strip()
            if dob and len(bin_val) == 12:
                parts = dob.split(".")
                if len(parts) == 3:
                    dd, mm, yy = parts[0].zfill(2), parts[1].zfill(2), parts[2][-2:]
                    expected = f"{yy}{mm}{dd}"
                    if bin_val[:6] == expected:
                        iin = bin_val
                        source = "ИП"
            elif bin_val:
                iin = bin_val
                source = "ИП (без проверки даты)"

        # Способ 2: ищем как директора
        if not iin:
            dir_records = fetch_by_director(fio)
            if dir_records:
                # Директор нам даёт только подтверждение что человек существует
                # Но не даёт ИИН напрямую (только БИН компании)
                source = f"директор в {dir_records[0].get('nameru', '')[:40]}"
                # Сохраняем без ИИН — это сигнал что человек активный предприниматель
                rows[idx]["iin_source"] = source

        if iin:
            rows[idx]["iin"] = iin
            rows[idx]["iin_source"] = source
            found_count += 1

        time.sleep(DELAY)

    # Финальное сохранение
    write_rows(OUTPUT_FILE, rows, columns)
    
    print(f"\n✅ ГОТОВО")
    print(f"   Обработано: {processed:,}")
    print(f"   Найдено ИИН: {found_count:,}")
    print(f"   Процент покрытия: {found_count/max(processed,1)*100:.1f}%")
    print(f"   💾 Сохранено: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
