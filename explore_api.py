"""
РАЗВЕДЧИК API data.egov.kz
Прощупывает все датасеты и ищет где есть поля ИИН / ФИО / физлица

Запуск: python explore_api.py
"""

import requests
import json
import time

BASE = "https://data.egov.kz"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

CANDIDATES = [
    "gbd_ul",
    "gbd_fl",
    "dara_kasipkerler_boiynsha_dere",
    "reestr_dolzh_po_isp_pr",
    "aisoip",
    "iin_registry",
    "fl_registry",
    "taxpayer",
    "kgd_ip",
    "ip_registry",
    "kazakstanyn____zandy_tulgalard",
    "notaries",
    "lawyers",
    "advokaty",
    "notarius",
    "medworkers",
    "teachers",
    "med_workers",
    "vrachi",
    "uchitelya",
    "pension",
    "pension_fund",
    "gbd_address",
    "residents",
    "zhiteli",
]

def check_mapping(index):
    for version in ["v1", "v2", "data"]:
        try:
            url = f"{BASE}/api/v4/mapping/{index}/{version}"
            r = requests.get(url, headers=HEADERS, timeout=10)
            if r.status_code == 200:
                return version, r.json()
        except:
            pass
    try:
        url = f"{BASE}/api/v4/mapping/{index}"
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return "noversion", r.json()
    except:
        pass
    return None, None

def get_sample(index, version="v1"):
    try:
        if version in ("noversion", None):
            url = f"{BASE}/api/detailed/{index}?source=" + json.dumps({"size": 2})
        else:
            url = f"{BASE}/api/detailed/{index}/{version}?source=" + json.dumps({"size": 2})
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and "data" in data:
                return data.get("totalCount", 0), data["data"]
            elif isinstance(data, list):
                return len(data), data
    except:
        pass
    return 0, []

def extract_fields(mapping_data):
    fields = []
    try:
        if isinstance(mapping_data, dict):
            for key, val in mapping_data.items():
                if isinstance(val, dict) and "mappings" in val:
                    for ver, ver_data in val["mappings"].items():
                        if "properties" in ver_data:
                            return list(ver_data["properties"].keys())
                elif isinstance(val, dict) and "properties" in val:
                    return list(val["properties"].keys())
    except:
        pass
    return fields

def score(fields, sample):
    iin_kw  = ["iin", "иин", "iinbin", "iin_bin"]
    fio_kw  = ["fio", "fullname", "name", "director", "lastname",
               "firstname", "surname", "famil"]
    fl = [f.lower() for f in fields]
    if sample:
        fl += [k.lower() for rec in sample for k in rec.keys()]
    has_iin = any(k in f for f in fl for k in iin_kw)
    has_fio = any(k in f for f in fl for k in fio_kw)
    return has_iin, has_fio

print("=" * 60)
print("РАЗВЕДЧИК API data.egov.kz")
print("=" * 60)

results = []

for index in CANDIDATES:
    print(f"\n🔍 {index}")
    version, mapping = check_mapping(index)
    if mapping is None:
        print(f"   ❌ 404")
        results.append({"index": index, "found": False})
        time.sleep(0.3)
        continue

    fields = extract_fields(mapping)
    total, sample = get_sample(index, version)
    if not fields and sample:
        fields = list(sample[0].keys()) if sample else []

    has_iin, has_fio = score(fields, sample)
    status = ("✅ ИИН+ФИО" if (has_iin and has_fio) else
              "🟡 ФИО"    if has_fio else
              "🟡 ИИН"    if has_iin else
              "⚪ нет")

    print(f"   {status} | записей: {total:,}")
    print(f"   поля: {', '.join(fields[:12])}")
    if sample:
        print(f"   пример: {json.dumps(sample[0], ensure_ascii=False)[:250]}")

    results.append({"index": index, "found": True, "version": version,
                    "total": total, "fields": fields,
                    "has_iin": has_iin, "has_fio": has_fio, "status": status})
    time.sleep(0.4)

print("\n" + "=" * 60)
print("ИТОГ:")
useful = [r for r in results if r.get("has_iin") or r.get("has_fio")]
if useful:
    for r in useful:
        print(f"  {r['status']} — {r['index']} ({r.get('total',0):,} записей)")
        print(f"     поля: {', '.join(r['fields'])}")
else:
    print("  ❌ ИИН физлиц в открытом доступе не найден")

with open("api_explore_result.json", "w", encoding="utf-8") as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print("\n💾 api_explore_result.json сохранён")