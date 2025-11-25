-- ============================================================
-- Criação da tabela de Indicadores
-- ============================================================

CREATE TABLE IF NOT EXISTS indicadores (
    nm_indicador TEXT PRIMARY KEY,
    vlr_indicador TEXT NOT NULL
);

-- Limpa a tabela antes de recarregar
DELETE FROM indicadores;

-- ============================================================
-- Indicador 1: Percentual de usuários de TI
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'pct_usuarios_ti',
    ROUND(
        (COUNT(*) * 100.0) /
        (SELECT COUNT(*) FROM professional_info),
        2
    )
FROM professional_info
WHERE department IN ('Engineering', 'Support', 'Research and Development');

-- ============================================================
-- Indicador 2: Idade média dos usuários
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'idade_media',
    ROUND(AVG(age), 2)
FROM users;

-- ============================================================
-- Indicador 3: Total de usuários na base
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'total_usuarios',
    COUNT(*)
FROM users;

-- ============================================================
-- Indicador 4: Percentual de usuários com menos de 30 anos
-- ============================================================

INSERT INTO indicadores (nm_indicador, vlr_indicador)
SELECT
    'pct_menos_30',
    ROUND(
        (COUNT(*) * 100.0) /
        (SELECT COUNT(*) FROM users),
        2
    )
FROM users
WHERE age < 30;

