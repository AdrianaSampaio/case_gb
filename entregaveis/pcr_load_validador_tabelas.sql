CREATE OR REPLACE PROCEDURE `casegrupoboticario.dev_zone.pcr_load_validador_tabelas`(
    p_src_table_full STRING, -- Ex: 'projeto_a.dataset_b.tabela_x'
    p_tgt_table_full STRING  -- Ex: 'projeto_c.dataset_d.tabela_y'
)
BEGIN

-- ===============================================
-- 1. CONFIGURAÇÃO (DEFINIÇÕES E EXTRAÇÃO DE VARIÁVEIS)
-- ===============================================

-- Variáveis de Projeto/Dataset
DECLARE src_project STRING;
DECLARE src_dataset STRING;
DECLARE src_name STRING;

DECLARE tgt_project STRING;
DECLARE tgt_dataset STRING;
DECLARE tgt_name STRING;

-- Variável de trabalho
DECLARE sql_checks_body STRING;

-- Extração Simplificada
SET src_project = SPLIT(p_src_table_full, '.')[OFFSET(0)];
SET src_dataset = SPLIT(p_src_table_full, '.')[OFFSET(1)];
SET src_name    = SPLIT(p_src_table_full, '.')[OFFSET(2)];

SET tgt_project = SPLIT(p_tgt_table_full, '.')[OFFSET(0)];
SET tgt_dataset = SPLIT(p_tgt_table_full, '.')[OFFSET(1)];
SET tgt_name    = SPLIT(p_tgt_table_full, '.')[OFFSET(2)];


-- ===============================================
-- 2. SCHEMA DRIFT CHECK
-- ===============================================

-- Cria TEMP TABLE schema_src
EXECUTE IMMEDIATE """
  CREATE TEMP TABLE schema_src AS
    SELECT
      column_name,
      data_type AS src_type
    FROM `"""|| src_project ||"""`."""|| src_dataset ||""".INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = ?
""" USING src_name; 

-- Cria TEMP TABLE schema_tgt
EXECUTE IMMEDIATE """
  CREATE TEMP TABLE schema_tgt AS
    SELECT
      column_name,
      data_type AS tgt_type
    FROM `"""|| tgt_project ||"""`."""|| tgt_dataset ||""".INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = ?
""" USING tgt_name; 

-- Cria TEMP TABLE schema_compare
EXECUTE IMMEDIATE """
  CREATE TEMP TABLE schema_compare AS
    SELECT
      COALESCE(s.column_name, t.column_name) AS column_name,
      s.src_type,
      t.tgt_type,
      CASE
        WHEN s.column_name IS NULL THEN 'MISSING_IN_SRC'
        WHEN t.column_name IS NULL THEN 'MISSING_IN_TGT'
        WHEN s.src_type != t.tgt_type THEN 'TYPE_MISMATCH'
        ELSE 'OK'
      END AS type_status
    FROM schema_src s
    FULL OUTER JOIN schema_tgt t USING (column_name)
"""; 

-- ===============================================
-- 3. MÉTRICAS DE QUALIDADE
-- ===============================================

-- ETAPA 1: CRIAR UMA TABELA TEMPORÁRIA COM A LISTA DE COLUNAS
EXECUTE IMMEDIATE FORMAT("""
    CREATE TEMP TABLE cols_to_check AS (
        SELECT column_name FROM `%s`.%s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'
        UNION DISTINCT 
        SELECT column_name FROM `%s`.%s.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'
    )
""", src_project, src_dataset, src_name, tgt_project, tgt_dataset, tgt_name);

-- ETAPA 2: CONSTRUIR O SQL DE CHECKS USANDO A TABELA TEMPORÁRIA
SET sql_checks_body = (
    SELECT
        STRING_AGG(
            FORMAT(
                """
                SELECT
                    '%s' AS column_name,
                    (SELECT COUNTIF(t1.`%s` IS NULL) FROM `%s` AS t1) AS src_nulls,
                    (SELECT COUNTIF(t2.`%s` IS NULL) FROM `%s` AS t2) AS tgt_nulls,
                    (SELECT COUNT(DISTINCT t1.`%s`) FROM `%s` AS t1) AS src_distinct,
                    (SELECT COUNT(DISTINCT t2.`%s`) FROM `%s` AS t2) AS tgt_distinct,
                    (SELECT COUNT(t1.`%s`) FROM `%s` AS t1) AS src_count,
                    (SELECT COUNT(t2.`%s`) FROM `%s` AS t2) AS tgt_count
                """,
                column_name, 
                column_name, p_src_table_full,
                column_name, p_tgt_table_full,
                column_name, p_src_table_full,
                column_name, p_tgt_table_full,
                column_name, p_src_table_full,
                column_name, p_tgt_table_full
            ),
            ' UNION ALL '
        )
    FROM cols_to_check 
    WHERE column_name IS NOT NULL
);

-- ETAPA 3: Criar TEMP TABLE checks
EXECUTE IMMEDIATE 
    CONCAT(
        'CREATE TEMP TABLE checks AS ',
        sql_checks_body
    );

-- ===============================================
-- 4. CRIAR E EXIBIR O RESULTADO FINAL
-- ===============================================

-- 4.1. Constrói a tabela temporária de resultados
CREATE TEMP TABLE final_results AS
SELECT
    -- Campos de Metadados
    CURRENT_TIMESTAMP() AS execution_time,
    p_src_table_full AS source_table,
    p_tgt_table_full AS target_table,
    
    -- Campos de Validação
    sc.column_name,
    sc.src_type,
    sc.tgt_type,
    sc.type_status,
    c.src_nulls,
    c.tgt_nulls,
    c.src_distinct,
    c.tgt_distinct,
    c.src_count,
    c.tgt_count,
    
    -- NOVO STATUS FINAL (Múltiplos Erros em uma String com termos didáticos)
    ARRAY_TO_STRING(
      ARRAY(
        SELECT err FROM UNNEST([
          -- 1. Comparação de Schema
          IF(sc.type_status != 'OK', CONCAT('Tipo de dado: ', sc.src_type, ' vs ', sc.tgt_type), NULL),
          -- 2. Comparação de Nulos
          IF(c.src_nulls != c.tgt_nulls, CONCAT('Valores nulos: ', c.src_nulls, ' vs ', c.tgt_nulls), NULL),
          -- 3. Comparação de Valores Distintos
          IF(c.src_distinct != c.tgt_distinct, CONCAT('Quantidade distinta: ', c.src_distinct, ' vs ', c.tgt_distinct), NULL),
          -- 4. Comparação de Contagem Total
          IF(c.src_count != c.tgt_count, CONCAT('Contagem total: ', c.src_count, ' vs ', c.tgt_count), NULL)
        ]) err
        WHERE err IS NOT NULL
      ), 
      '; ' -- Separador alterado para ponto e vírgula para melhor leitura
    ) AS validation_errors_list,
    
    -- Status Geral (OK ou ERROR, baseado na lista)
    CASE 
      WHEN sc.type_status != 'OK' OR 
           c.src_nulls != c.tgt_nulls OR 
           c.src_distinct != c.tgt_distinct OR 
           c.src_count != c.tgt_count 
      THEN 'ERROR'
      ELSE 'OK'
    END AS final_status

FROM schema_compare sc
LEFT JOIN checks c USING (column_name)
ORDER BY final_status DESC, column_name;


END;