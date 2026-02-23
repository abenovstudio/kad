"""
ГЕНЕРАТОР ИИН-ПРЕФИКСОВ
Входной файл: CSV с колонками last_name, first_name, patronymic, extra, gender, date_birth, place
Выходной файл: тот же + колонка iin_prefix

Запуск: python generate_iin.py --input ваш_файл.csv --output result.csv
Фильтр по Павлодару: python generate_iin.py --input ваш_файл.csv --place ПАВЛОДАР
"""

import argparse
import csv
import sys
from pathlib import Path


def parse_date(date_str):
    if date_str is None or str(date_str).strip() == '':
        return None
    s = str(date_str).strip()
    parts = s.split('.')
    if len(parts) != 3:
        return None
    try:
        day   = int(parts[0])
        month = int(parts[1])
        year  = int(parts[2])
        if year < 100:
            year = 2000 + year if year <= 24 else 1900 + year
        return (day, month, year)
    except ValueError:
        return None


def gender_digit(gender_str, year):
    if gender_str is None or str(gender_str).strip() == '':
        return '*'
    g = str(gender_str).strip().upper()
    is_male   = g in ('М', 'M', 'МУЖ', 'МУЖСКОЙ', 'MALE')
    is_female = g in ('Ж', 'F', 'ЖЕН', 'ЖЕНСКИЙ', 'FEMALE')
    if is_male:
        return '3' if year >= 2000 else '1'
    elif is_female:
        return '4' if year >= 2000 else '2'
    else:
        return '*'


def build_iin_prefix(row):
    parsed = parse_date(row.get('date_birth', ''))
    if not parsed:
        return 'ERR_DATE'
    day, month, year = parsed
    yy = str(year)[-2:].zfill(2)
    mm = str(month).zfill(2)
    dd = str(day).zfill(2)
    g  = gender_digit(row.get('gender', ''), year)
    return f"{yy}{mm}{dd}{g}00000"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',  '-i', default='vdg-pvl-list.csv')
    parser.add_argument('--output', '-o', default='result_with_iin.csv')
    parser.add_argument('--sep',    '-s', default=',')
    parser.add_argument('--place',  '-p', default='')
    args = parser.parse_args()

    if not Path(args.input).exists():
        print(f"❌ Файл не найден: {args.input}")
        sys.exit(1)

    print(f"📂 Читаем: {args.input}")
    rows = None
    columns = []
    for enc in ['utf-8', 'utf-8-sig', 'cp1251', 'windows-1251']:
        try:
            with open(args.input, 'r', encoding=enc, newline='') as f:
                reader = csv.DictReader(f, delimiter=args.sep)
                columns = list(reader.fieldnames or [])
                rows = list(reader)
            print(f"✅ Кодировка: {enc}, строк: {len(rows):,}")
            break
        except Exception:
            continue

    if rows is None:
        print("❌ Не удалось прочитать файл")
        sys.exit(1)

    print(f"📋 Колонки: {columns}")

    if args.place and 'place' in columns:
        before = len(rows)
        place_upper = args.place.upper()
        rows = [r for r in rows if place_upper in str(r.get('place', '')).upper()]
        print(f"🏙️  Фильтр '{args.place}': {len(rows):,} из {before:,}")

    print("⚙️  Генерируем ИИН-префиксы...")
    for row in rows:
        row['iin_prefix'] = build_iin_prefix(row)

    errors = sum(1 for r in rows if str(r.get('iin_prefix', '')).startswith('ERR'))
    unknown = sum(1 for r in rows if '*' in str(r.get('iin_prefix', '')))
    success = len(rows) - errors

    print(f"\n📈 Результат:")
    print(f"   ✅ Успешно:          {success:,}")
    print(f"   ❓ Пол неизвестен:  {unknown:,}  (7-я цифра = *)")
    print(f"   ❌ Ошибка даты:      {errors:,}")

    output_columns = list(columns)
    if 'iin_prefix' not in output_columns:
        output_columns.append('iin_prefix')
    with open(args.output, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=output_columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n💾 Сохранено: {args.output}")

    print(f"\n📝 Примеры:")
    examples = [r for r in rows if r.get('iin_prefix') != 'ERR_DATE'][:5]
    for row in examples:
        name = f"{row.get('last_name','')} {row.get('first_name','')} {row.get('patronymic','')}"
        print(f"   {name.strip():<35} {row.get('date_birth',''):<12} → {row['iin_prefix']}")


if __name__ == '__main__':
    main()
