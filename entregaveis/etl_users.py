import requests
import sqlite3
import json
import os
from datetime import datetime

# =========================================
# CONFIGURAÇÃO DO CAMINHO DO BANCO
# =========================================
# Garante que o banco será criado na pasta raíz do projeto (case_gb)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "users.db")

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
# 2. CRIA BANCO E TABELAS
# =========================================
def create_database(db_path=DB_PATH):

    print(f"💾 Criando banco em: {db_path}")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    print("💾 Criando tabelas RAW e TRUSTED...")

    # RAW
    cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_users (
            raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            json_data TEXT,
            ingestion_timestamp TEXT
        );
    """)

    # TRUSTED
    cur.execute("DROP TABLE IF EXISTS users;")
    cur.execute("DROP TABLE IF EXISTS professional_info;")
    cur.execute("DROP TABLE IF EXISTS address;")
    cur.execute("DROP TABLE IF EXISTS bank;")

    cur.execute("""
        CREATE TABLE users (
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
        CREATE TABLE professional_info (
            user_id INTEGER,
            company TEXT,
            department TEXT,
            title TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE address (
            user_id INTEGER,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            address TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)

    cur.execute("""
        CREATE TABLE bank (
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
            INSERT INTO raw_users (user_id, json_data, ingestion_timestamp)
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
        FROM raw_users r
        INNER JOIN (
            SELECT user_id, MAX(raw_id) AS max_raw
            FROM raw_users
            GROUP BY user_id
        ) t
        ON r.user_id = t.user_id AND r.raw_id = t.max_raw
    """

    rows = cur.execute(query).fetchall()

    for user_id, json_blob in rows:
        u = json.loads(json_blob)

        # USERS
        cur.execute("""
            INSERT INTO users (id, first_name, last_name, age, gender, email, phone)
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

        # PROFESSIONAL INFO
        cur.execute("""
            INSERT INTO professional_info (user_id, company, department, title)
            VALUES (?, ?, ?, ?)
        """, (
            u["id"],
            u["company"]["name"],
            u["company"]["department"],
            u["company"]["title"]
        ))

        # ADDRESS
        cur.execute("""
            INSERT INTO address (user_id, city, state, postal_code, address)
            VALUES (?, ?, ?, ?, ?)
        """, (
            u["id"],
            u["address"]["city"],
            u["address"]["state"],
            u["address"]["postalCode"],
            u["address"]["address"]
        ))

        # BANK
        cur.execute("""
            INSERT INTO bank (user_id, card_type, card_number)
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
    conn = create_database()
    insert_into_raw(conn, users)
    populate_trusted(conn)
    conn.close()

    print("\n🎉 Pipeline RAW → TRUSTED finalizado com sucesso!")
    print(f"📂 Banco salvo em: {DB_PATH}")
