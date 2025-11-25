import pandas as pd
from sqlalchemy import create_engine, text

DB_PATH = "users.db"
engine = create_engine(f"sqlite:///{DB_PATH}")

print("📊 Gerando tabela de indicadores...")

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS indicadores (
    nm_indicador TEXT PRIMARY KEY,
    vlr_indicador TEXT NOT NULL
);
"""
DELETE_TABLE = "DELETE FROM indicadores;"

with engine.begin() as conn:
    conn.execute(text(CREATE_TABLE))
    conn.execute(text(DELETE_TABLE))

# =====================================================
# Cálculo dos indicadores
# =====================================================

df_users = pd.read_sql("SELECT * FROM users", engine)
df_prof = pd.read_sql("SELECT * FROM professional_info", engine)

indicadores = {
    "pct_usuarios_ti": round(
        len(df_prof[df_prof["department"].isin(["Engineering","Support","Research and Development"])]) 
        * 100 / len(df_prof), 
        2
    ),
    "idade_media": round(df_users["age"].mean(), 2),
    "total_usuarios": len(df_users),
    "pct_menos_30": round((df_users[df_users["age"] < 30].shape[0] * 100) / len(df_users), 2)
}

# Grava no SQLite
ind_df = pd.DataFrame([
    {"nm_indicador": k, "vlr_indicador": str(v)}
    for k, v in indicadores.items()
])

ind_df.to_sql("indicadores", engine, if_exists="append", index=False)

print("✔ Indicadores gerados com sucesso!")

# =====================================================
# EXIBIÇÃO FORMATADA DOS INDICADORES
# =====================================================

# 1) Ler a tabela de indicadores
df_ind = pd.read_sql("SELECT nm_indicador, vlr_indicador FROM indicadores", engine)

# 2) Normalizar tipos e formatar percentuais
def format_value(row):
    name = row["nm_indicador"]
    val = row["vlr_indicador"]

    # tenta converter para float/inteiro
    try:
        num = float(val)
    except Exception:
        return val  # se não for numérico, retorna como está

    # se for percentual (nome contendo 'pct' ou 'percent'), formatar com %
    if "pct" in name.lower() or "percent" in name.lower():
        # remove .0 desnecessário para inteiros, mantém 2 casas para decimais
        if abs(num - int(num)) < 1e-9:
            return f"{int(num)}%"
        else:
            return f"{num:.2f}%"
    else:
        # para contagens inteiras, mostrar sem casas decimais
        if abs(num - int(num)) < 1e-9:
            return str(int(num))
        # para médias/valores numéricos, mostrar com 2 casas
        return f"{num:.2f}"

df_ind["vlr_formatado"] = df_ind.apply(format_value, axis=1)

# 3) Imprimir tabela estética no console
col1_width = max(df_ind["nm_indicador"].str.len().max(), len("nm_indicador"))
col2_width = max(df_ind["vlr_formatado"].str.len().max(), len("vlr_indicador"))

sep = "+-" + "-" * col1_width + "-+-" + "-" * col2_width + "-+"
header = "| " + "nm_indicador".ljust(col1_width) + " | " + "vlr_indicador".ljust(col2_width) + " |"

print("\n=== Indicadores Calculados ===")
print(sep)
print(header)
print(sep)
for _, r in df_ind.iterrows():
    print("| " + r["nm_indicador"].ljust(col1_width) + " | " + r["vlr_formatado"].rjust(col2_width) + " |")
print(sep)