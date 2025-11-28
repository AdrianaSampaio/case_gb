import requests
import sqlite3
import json
import os
from datetime import datetime

# =========================================
# CONFIGURAÇÃO DOS CAMINHOS DOS BANCOS
# =========================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DB_PATH = os.path.join(BASE_DIR, "raw.db")
TRUSTED_DB_PATH = os.path.join(BASE_DIR, "trusted.db")

# =========================================
# 1. BAIXA DADOS DA API
# =========================================
def fetch_users():
    print("📡 Lendo API https://dummyjson.com/users ...")
    response = requests.get("https://dummyjson.com/users?limit=200")

    if response.status_code != 200:
        raise Exception("Erro ao consultar API")

    data = response.json()["users"]
    print(f"✔ Recebidos {len(data)} usuários")
    return data

# =========================================
# 2. CRIA RAW E TRUSTED (DOIS DATASETS)
# =========================================
def setup_databases():

    print("📦 Criando datasets raw.db e trusted.db")
    
    # Conexão principal (pode ser qualquer uma)
    conn = sqlite3.connect(os.path.join(BASE_DIR, "dev.db"))
    cur = conn.cursor()

    # Attach RAW
    cur.execute(f"ATTACH DATABASE '{RAW_DB_PATH}' AS raw;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.users (
            raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            json_data TEXT,
            ingestion_timestamp TEXT
        );
    """)

    # Attach TRUSTED
    cur.execute(f"ATTACH DATABASE '{TRUSTED_DB_PATH}' AS trusted;")

    cur.execute("DROP TABLE IF EXISTS trusted.users;")
    cur.execute("DROP TABLE IF EXISTS trusted.professional_info;")
    cur.execute("DROP TABLE IF EXISTS trusted.address;")
    cur.execute("DROP TABLE IF EXISTS trusted.bank;")

    cur.execute("""
        CREATE TABLE trusted.users (
            id INTEGER PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            gender TEXT,
            email TEXT,
            phone TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE trusted.professional_info (
            user_id INTEGER,
            company TEXT,
            department TEXT,
            title TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE trusted.address (
            user_id INTEGER,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            address TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE trusted.bank (
            user_id INTEGER,
            card_type TEXT,
            card_number TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    conn.commit()
    return conn

# =========================================
# 3. INSERE DADOS NA RAW
# =========================================
def insert_into_raw(conn, users):
    cur = conn.cursor()
    now = datetime.utcnow().isoformat()

    print("📥 Gravando registros na RAW...")

    for u in users:
        cur.execute("""
            INSERT INTO raw.users (user_id, json_data, ingestion_timestamp)
            VALUES (?, ?, ?)
        """, (
            u["id"],
            json.dumps(u),
            now
        ))

    conn.commit()
    print("✔ RAW atualizada")

# =========================================
# 4. GERAR TRUSTED A PARTIR DA RAW
# =========================================
def populate_trusted(conn):
    cur = conn.cursor()

    print("⚙ Processando RAW → TRUSTED")

    query = """
        SELECT r.user_id, r.json_data
        FROM raw.users r
        INNER JOIN (
            SELECT user_id, MAX(raw_id) AS max_raw
            FROM raw.users
            GROUP BY user_id
        ) t
        ON r.user_id = t.user_id AND r.raw_id = t.max_raw
    """

    rows = cur.execute(query).fetchall()

    for user_id, json_blob in rows:
        u = json.loads(json_blob)

        # trusted.users
        cur.execute("""
            INSERT INTO trusted.users (id, first_name, last_name, age, gender, email, phone)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            u["id"],
            u["firstName"],
            u["lastName"],
            u["age"],
            u["gender"],
            u["email"],
            u["phone"]
        ))

        # trusted.professional_info
        cur.execute("""
            INSERT INTO trusted.professional_info (user_id, company, department, title)
            VALUES (?, ?, ?, ?)
        """, (
            u["id"],
            u["company"]["name"],
            u["company"]["department"],
            u["company"]["title"]
        ))

        # trusted.address
        cur.execute("""
            INSERT INTO trusted.address (user_id, city, state, postal_code, address)
            VALUES (?, ?, ?, ?, ?)
        """, (
            u["id"],
            u["address"]["city"],
            u["address"]["state"],
            u["address"]["postalCode"],
            u["address"]["address"]
        ))

        # trusted.bank
        cur.execute("""
            INSERT INTO trusted.bank (user_id, card_type, card_number)
            VALUES (?, ?, ?)
        """, (
            u["id"],
            u["bank"]["cardType"],
            u["bank"]["cardNumber"]
        ))

    conn.commit()
    print("✔ TRUSTED gerado com sucesso")

# =========================================
# MAIN
# =========================================
if __name__ == "__main__":
    users = fetch_users()
    conn = setup_databases()
    insert_into_raw(conn, users)
    populate_trusted(conn)
    conn.close()

    print("\n🎉 Pipeline RAW → TRUSTED finalizado com sucesso!")
    print("📂 RAW em:", RAW_DB_PATH)
    print("📂 TRUSTED em:", TRUSTED_DB_PATH)
