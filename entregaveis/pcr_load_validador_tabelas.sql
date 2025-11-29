CREATE OR REPLACE PROCEDURE `casegrupoboticario.dev_zone.pcr_load_validador_tabelas`(p_src_table_full STRING, p_src_pk STRING, p_tgt_table_full STRING, p_tgt_pk STRING)
BEGIN

/* ===========================================================
   1. CONFIGURAÇÃO E EXTRAÇÃO DE PROJECT / DATASET / TABLE
   =========================================================== */

DECLARE src_project STRING;
DECLARE src_dataset STRING;
DECLARE src_name STRING;

DECLARE tgt_project STRING;
DECLARE tgt_dataset STRING;
DECLARE tgt_name STRING;

DECLARE sql_checks_body STRING;
DECLARE sql_pk_divergent STRING;

SET src_project = SPLIT(p_src_table_full, '.')[OFFSET(0)];
SET src_dataset = SPLIT(p_src_table_full, '.')[OFFSET(1)];
SET src_name    = SPLIT(p_src_table_full, '.')[OFFSET(2)];

SET tgt_project = SPLIT(p_tgt_table_full, '.')[OFFSET(0)];
SET tgt_dataset = SPLIT(p_tgt_table_full, '.')[OFFSET(1)];
SET tgt_name    = SPLIT(p_tgt_table_full, '.')[OFFSET(2)];

/* ===========================================================
   2. SCHEMA DRIFT CHECK
   =========================================================== */

EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE schema_src AS
  SELECT column_name, data_type AS src_type
  FROM `%s.%s`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name = '%s'
""", src_project, src_dataset, src_name);

EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE schema_tgt AS
  SELECT column_name, data_type AS tgt_type
  FROM `%s.%s`.INFORMATION_SCHEMA.COLUMNS
  WHERE table_name = '%s'
""", tgt_project, tgt_dataset, tgt_name);

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

/* ===========================================================
   3. LISTA DE COLUNAS
   =========================================================== */

EXECUTE IMMEDIATE FORMAT("""
  CREATE TEMP TABLE cols_to_check AS (
    SELECT column_name FROM `%s.%s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'
    UNION DISTINCT
    SELECT column_name FROM `%s.%s`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '%s'
  )
""",
src_project, src_dataset, src_name,
tgt_project, tgt_dataset, tgt_name);

/* ===========================================================
   4. MÉTRICAS PADRÃO (NULLS, DISTINCTS, COUNT)
   =========================================================== */

SET sql_checks_body = (
    SELECT STRING_AGG(
        FORMAT("""
            SELECT
                '%s' AS column_name,
                (SELECT COUNTIF(t1.%s IS NULL) FROM `%s` t1) AS src_nulls,
                (SELECT COUNTIF(t2.%s IS NULL) FROM `%s` t2) AS tgt_nulls,
                (SELECT COUNT(DISTINCT t1.%s) FROM `%s` t1) AS src_distinct,
                (SELECT COUNT(DISTINCT t2.%s) FROM `%s` t2) AS tgt_distinct,
                (SELECT COUNT(t1.%s) FROM `%s` t1) AS src_count,
                (SELECT COUNT(t2.%s) FROM `%s` t2) AS tgt_count
        """,
        column_name,
        column_name, p_src_table_full,
        column_name, p_tgt_table_full,
        column_name, p_src_table_full,
        column_name, p_tgt_table_full,
        column_name, p_src_table_full,
        column_name, p_tgt_table_full
        ),
        " UNION ALL "
    )
    FROM cols_to_check
);

EXECUTE IMMEDIATE (
    "CREATE TEMP TABLE checks AS " || sql_checks_body
);

/* ===========================================================
   5. DIVERGÊNCIAS POR PK (FULL OUTER JOIN + PK DIFERENTE)
   =========================================================== */

-- FULL OUTER JOIN compara registros SRC e TGT mesmo quando só existe em uma das tabelas
SET sql_pk_divergent = (
    SELECT STRING_AGG(
        FORMAT("""
            SELECT
                '%s' AS column_name,
                COUNT(*) AS divergent
            FROM (
                SELECT
                    s.%s AS src_pk,
                    t.%s AS tgt_pk,
                    SAFE_CAST(s.%s AS STRING) AS src_val,
                    SAFE_CAST(t.%s AS STRING) AS tgt_val
                FROM `%s` s
                FULL OUTER JOIN `%s` t
                    ON s.%s = t.%s
            )
            WHERE (src_val IS DISTINCT FROM tgt_val)
        """,
        column_name,
        p_src_pk,
        p_tgt_pk,
        column_name,
        column_name,
        p_src_table_full,
        p_tgt_table_full,
        p_src_pk,
        p_tgt_pk
        ),
        " UNION ALL "
    )
    FROM cols_to_check
);

EXECUTE IMMEDIATE (
    "CREATE TEMP TABLE pk_divergent AS " || sql_pk_divergent
);

/* ===========================================================
   6. RESULTADO FINAL
   =========================================================== */

CREATE TEMP TABLE final_results AS
SELECT
    CURRENT_TIMESTAMP() AS execution_time,
    p_src_table_full AS source_table,
    p_tgt_table_full AS target_table,

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

    COALESCE(d.divergent, 0) AS divergent,

    ARRAY_TO_STRING(
      ARRAY(
        SELECT err FROM UNNEST([

          IF(sc.type_status != 'OK',
             CONCAT('Tipo de dado: ', sc.src_type, ' vs ', sc.tgt_type), NULL),

          IF(c.src_nulls != c.tgt_nulls,
             CONCAT('Valores nulos: ', c.src_nulls, ' vs ', c.tgt_nulls), NULL),

          IF(c.src_distinct != c.tgt_distinct,
             CONCAT('Distintos: ', c.src_distinct, ' vs ', c.tgt_distinct), NULL),

          IF(c.src_count != c.tgt_count,
             CONCAT('Contagem total: ', c.src_count, ' vs ', c.tgt_count), NULL),

          IF(COALESCE(d.divergent,0) > 0,
             CONCAT('Divergências por PK: ', d.divergent), NULL)

        ]) err
        WHERE err IS NOT NULL
      ), 
      '; '
    ) AS validation_errors_list,

    CASE 
      WHEN sc.type_status != 'OK'
        OR c.src_nulls != c.tgt_nulls
        OR c.src_distinct != c.tgt_distinct
        OR c.src_count != c.tgt_count
        OR COALESCE(d.divergent,0) > 0
      THEN 'ERROR'
      ELSE 'OK'
    END AS final_status

FROM schema_compare sc
LEFT JOIN checks c USING (column_name)
LEFT JOIN pk_divergent d USING (column_name)
ORDER BY final_status DESC, column_name;

END;