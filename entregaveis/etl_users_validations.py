import os
import pandas as pd
from sqlalchemy import create_engine

# ========================================================
#  Localiza automaticamente os bancos correctos
#  - raw.db        → contém users
#  - trusted.db    → contém users, professional_info, address, bank
# ========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_DB_PATH = os.path.join(BASE_DIR, "raw.db")
TRUSTED_DB_PATH = os.path.join(BASE_DIR, "trusted.db")

print(f"📂 Banco RAW:      {RAW_DB_PATH}")
print(f"📂 Banco TRUSTED:  {TRUSTED_DB_PATH}")

engine_raw = create_engine(f"sqlite:///{RAW_DB_PATH}")
engine_trusted = create_engine(f"sqlite:///{TRUSTED_DB_PATH}")


def query(sql, source="trusted"):
    """Executa SQL no banco selecionado."""
    engine = engine_trusted if source == "trusted" else engine_raw
    return pd.read_sql(sql, engine)


print("\n===== 🔍 VALIDAÇÃO DA BASE (RAW → TRUSTED) =====")

# =======================================================
# 1. Totais por tabela
# =======================================================
print("\n📌 Totais por tabela:")
print(query("""
SELECT 'users' AS tabela, COUNT(*) AS total FROM users
UNION ALL
SELECT 'professional_info', COUNT(*) FROM professional_info
UNION ALL
SELECT 'address', COUNT(*) FROM address
UNION ALL
SELECT 'bank', COUNT(*) FROM bank
""", source="trusted"))

print(query("""
SELECT 'raw_users' AS tabela, COUNT(*) AS total FROM users
""", source="raw"))

# =======================================================
# 2. IDs duplicados em users
# =======================================================
print("\n📌 IDs duplicados em users (não deveria haver):")
print(query("""
SELECT id, COUNT(*) AS qtd
FROM users
GROUP BY id
HAVING COUNT(*) > 1;
""", source="trusted"))

# =======================================================
# 3. Usuários sem endereço
# =======================================================
print("\n📌 Users sem endereço:")
print(query("""
SELECT u.id
FROM users u
LEFT JOIN address a ON u.id = a.user_id
WHERE a.user_id IS NULL;
""", source="trusted"))

# =======================================================
# 4. Usuários sem informação profissional
# =======================================================
print("\n📌 Users sem professional_info:")
print(query("""
SELECT u.id
FROM users u
LEFT JOIN professional_info p ON u.id = p.user_id
WHERE p.user_id IS NULL;
""", source="trusted"))

# =======================================================
# 5. Distribuição por gênero
# =======================================================
print("\n📌 Distribuição por gênero:")
print(query("""
SELECT gender, COUNT(*) AS total
FROM users
GROUP BY gender;
""", source="trusted"))

# =======================================================
# 6. Departamentos
# =======================================================
print("\n📌 Distribuição por departamento:")
print(query("""
SELECT department, COUNT(*) AS total
FROM professional_info
GROUP BY department
ORDER BY total DESC;
""", source="trusted"))

# =======================================================
# 7. Profissionais de TI < 40 anos
# =======================================================
print("\n📌 Percentual de profissionais de TI com menos de 40 anos:")
print(query("""
WITH classificacao AS (
    SELECT 
        CASE
            WHEN p.department IN ('Support', 'Engineering', 'Research and Development')
                 AND u.age < 40
            THEN TRUE
            ELSE FALSE
        END AS atende
    FROM users u
    JOIN professional_info p 
        ON u.id = p.user_id
)

SELECT 
    atende,
    COUNT(*) AS qtd,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentual
FROM classificacao
GROUP BY atende;
""", source="trusted"))

print("\n===== ✔ VALIDAÇÃO FINALIZADA =====\n")
