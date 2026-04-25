import requests
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

print("=" * 60)
print("Pobranie danych z API REST Countries")
print("=" * 60)

url = "https://restcountries.com/v3.1/all?fields=name,capital,region,subregion,population,area,currencies"

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()
    print(f"Pobrano dane o {len(data)} krajach.")
except requests.exceptions.RequestException as e:
    print(f"Błąd pobierania danych: {e}")
    exit(1)

def get_currency(currencies_dict):
    if currencies_dict:
        return list(currencies_dict.keys())[0]
    return None

kraje_lista = []

for kraj in data:
    kraje_lista.append({
        "nazwa":        kraj.get("name", {}).get("common", None),
        "stolica":      (kraj.get("capital") or [None])[0],
        "region":       kraj.get("region", None),
        "subregion":    kraj.get("subregion", None),
        "populacja":    kraj.get("population", None),
        "powierzchnia": kraj.get("area", None),
        "waluta":       get_currency(kraj.get("currencies", None)),
    })

df = pd.DataFrame(kraje_lista)

print(f"\n--- df.head() ---")
print(df.head(10))

print(f"\n--- df.shape ---")
print(f"Rozmiar: {df.shape[0]} wierszy × {df.shape[1]} kolumn")

print(f"\n--- df.dtypes ---")
print(df.dtypes)


print("\n" + "=" * 60)
print("Zapis do bazy SQLite")
print("=" * 60)

conn = sqlite3.connect("kraje_swiata.db")

df.to_sql("kraje", conn, if_exists="replace", index=False)

print("Tabela 'kraje' zapisana w bazie 'kraje_swiata.db'.")


print("\n" + "=" * 60)
print("Analiza SQL")
print("=" * 60)

print("\n--- 1. Łączna populacja świata ---")
df_q1 = pd.read_sql_query("""
    SELECT SUM(populacja) AS laczna_populacja
    FROM kraje
""", conn)
print(df_q1)

print("\n--- 2. Top 10 krajów o największej populacji ---")
df_q2 = pd.read_sql_query("""
    SELECT nazwa, populacja
    FROM kraje
    ORDER BY populacja DESC
    LIMIT 10
""", conn)
print(df_q2)

print("\n--- 3. Liczba krajów w regionach i średnia populacja ---")
df_q3 = pd.read_sql_query("""
    SELECT
        region,
        COUNT(*)                    AS liczba_krajow,
        ROUND(AVG(populacja), 0)    AS srednia_populacja
    FROM kraje
    WHERE region IS NOT NULL AND region != ''
    GROUP BY region
    ORDER BY liczba_krajow DESC
""", conn)
print(df_q3)

print("\n--- 4. Kraje o powierzchni większej niż Polska (312 679 km²) ---")
df_q4 = pd.read_sql_query("""
    SELECT nazwa, powierzchnia
    FROM kraje
    WHERE powierzchnia > 312679
    ORDER BY powierzchnia DESC
""", conn)
print(df_q4)

print("\n--- 5. Top 10 krajów wg gęstości zaludnienia (populacja / powierzchnia) ---")
df_q5 = pd.read_sql_query("""
    SELECT
        nazwa,
        populacja,
        powierzchnia,
        ROUND(populacja * 1.0 / powierzchnia, 2) AS gestosc
    FROM kraje
    WHERE powierzchnia > 0
    ORDER BY gestosc DESC
    LIMIT 10
""", conn)
print(df_q5)

print("\n" + "=" * 60)
print("Wizualizacja")
print("=" * 60)

df_wykres = pd.read_sql_query("""
    SELECT
        region,
        SUM(populacja) AS laczna_populacja
    FROM kraje
    WHERE region IS NOT NULL AND region != ''
    GROUP BY region
    ORDER BY laczna_populacja DESC
""", conn)

conn.close()

plt.figure(figsize=(10, 6))
plt.bar(
    df_wykres["region"],
    df_wykres["laczna_populacja"] / 1_000_000_000,
    color="#4C72B0"
)
plt.title("Łączna populacja każdego regionu świata", fontsize=14)
plt.xlabel("Region", fontsize=12)
plt.ylabel("Populacja (mld)", fontsize=12)
plt.xticks(rotation=25, ha="right")
plt.grid(axis="y", alpha=0.3)
plt.tight_layout()

plt.savefig("populacja_regionow.png", dpi=150)
plt.show()
print("Wykres zapisany jako 'populacja_regionow.png'.")

print("\n" + "=" * 60)
print("Pipeline zakończony pomyślnie!")
print("=" * 60)
