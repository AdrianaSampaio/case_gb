-- ============================================================
-- Conecta ao banco dev.db (o arquivo é criado se não existir)
-- ============================================================

-- Anexa o banco trusted.db para permitir leitura
ATTACH DATABASE 'C:/Users/nanyn/OneDrive/Documentos/case_gb/trusted.db' AS trusted;

-- ============================================================
-- Criação da tabela de Indicadores em dev.db
-- ============================================================

CREATE TABLE IF NOT EXISTS indicadores (
    nm_indicador TEXT PRIMARY KEY,
    vlr_indicador TEXT NOT NULL
);

-- Limpa os dados antes de recarregar
DELETE FROM indicadores;

-- ============================================================
-- Indicador 1: Percentual de usuários de TI
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'pct_usuarios_ti',
    ROUND(
        (COUNT(*) * 100.0) /
        (SELECT COUNT(*) FROM trusted.professional_info),
        2
    )
FROM trusted.professional_info
WHERE department IN ('Engineering', 'Support', 'Research and Development');

-- ============================================================
-- Indicador 2: Idade média dos usuários
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'idade_media',
    ROUND(AVG(age), 2)
FROM trusted.users;

-- ============================================================
-- Indicador 3: Total de usuários na base
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'total_usuarios',
    COUNT(*)
FROM trusted.users;

-- ============================================================
-- Indicador 4: Percentual de usuários com menos de 30 anos
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'pct_menos_30',
    ROUND(
        (COUNT(*) * 100.0) /
        (SELECT COUNT(*) FROM trusted.users),
        2
    )
FROM trusted.users
WHERE age < 30;

-- ============================================================
-- Desanexa trusted.db
-- ============================================================

DETACH DATABASE trusted;
