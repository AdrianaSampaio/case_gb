CREATE OR REPLACE PROCEDURE `casegrupoboticario.dev_zone.pcr_load_tb_validation`()

BEGIN 

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE tmp_application_record_gcp_trat AS
    SELECT 
      CAST(id AS INT64) AS id,
      SUBSTR(code_gender, 1, 1) AS code_gender,  -- padronizando para F e M conforme local
      CASE
        WHEN flag_own_car ='Y' THEN TRUE
        WHEN flag_own_car ='N' THEN FALSE
      END AS flag_own_car, -- padronizando flags para true e false
      CASE
        WHEN flag_own_realty ='Y' THEN TRUE
         WHEN flag_own_realty ='N' THEN FALSE
      END AS flag_own_realty, -- padronizando flags para true e false
      CAST(cnt_children AS INT64) AS cnt_children, --padronizando inteiro
      CAST(amt_income_total AS NUMERIC)/100 AS amt_income_total, -- resolvendo questoes de valores que subiram *100 (considerando que foi checado na fonte)
      name_income_type,
      name_education_type,
      name_family_status,
      name_housing_type,
      CAST(days_birth AS INT64)*-1 AS days_birth,  -- padronizando inteiro e negativo conforme tabela local
      CAST(days_employed AS INT64) AS days_employed, -- padronizando inteiro
      CASE
        WHEN CAST(flag_mobil AS INT64) =1 THEN TRUE
        WHEN CAST(flag_mobil AS INT64) =0 THEN FALSE
      END AS flag_mobil, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_work_phone AS FLOAT64) =1 THEN TRUE
        WHEN CAST(flag_work_phone AS FLOAT64) =0 THEN FALSE
      END AS flag_work_phone, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_phone AS INT64) =1 THEN TRUE
        WHEN CAST(flag_phone AS INT64) =0 THEN FALSE
      END AS flag_phone, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_email AS INT64) =1 THEN TRUE
        WHEN CAST(flag_email AS INT64) =0 THEN FALSE
      END AS flag_email, -- padronizando flags para true e false
      occupation_type,
      CAST(CAST(cnt_fam_members AS FLOAT64) AS INT64) AS cnt_fam_members --padronizando inteiro

    FROM `casegrupoboticario.raw_zone.application_record_gcp`
""";


EXECUTE IMMEDIATE """
  CREATE TEMP TABLE tmp_application_record_local_trat AS
    SELECT 
      CAST(id AS INT64) AS id,
      SUBSTR(code_gender, 1, 1) AS code_gender,  
      CASE
        WHEN flag_own_car ='Y' THEN TRUE
        WHEN flag_own_car ='N' THEN FALSE
      END AS flag_own_car, -- padronizando flags para true e false
      CASE
        WHEN flag_own_realty ='Y' THEN TRUE
        WHEN flag_own_realty ='N' THEN FALSE
      END AS flag_own_realty, -- padronizando flags para true e false
      CAST(cnt_children AS INT64) AS cnt_children, --padronizando inteiro
      CAST(amt_income_total AS NUMERIC) AS amt_income_total, -- padronizando numeric 
      name_income_type,
      name_education_type,
      name_family_status,
      name_housing_type,
      CAST(days_birth AS INT64) as days_birth, --padronizando inteiro
      CAST(days_employed AS INT64) as days_employed, --padronizando inteiro
      CASE
        WHEN CAST(flag_mobil AS INT64) =1 THEN TRUE
        WHEN CAST(flag_mobil AS INT64) =0 THEN FALSE
      END AS flag_mobil, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_work_phone AS FLOAT64) =1 THEN TRUE
        ELSE FALSE
      END AS flag_work_phone, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_phone AS INT64) =1 THEN TRUE
        WHEN CAST(flag_phone AS INT64) =0 THEN FALSE
      END AS flag_phone, -- padronizando flags para true e false
      CASE
        WHEN CAST(flag_email AS INT64) =1 THEN TRUE
        WHEN CAST(flag_email AS INT64) =0 THEN FALSE
      END AS flag_email, -- padronizando flags para true e false
      occupation_type,
      CAST(CAST(cnt_fam_members AS FLOAT64) AS INT64) AS cnt_fam_members --padronizando inteiro

    FROM `casegrupoboticario.raw_zone.application_record_local`
""";

EXECUTE IMMEDIATE """
  CREATE TEMP TABLE tmp_tb_validation AS
    SELECT
      COALESCE(GCP.id, LCL.id) AS id,
      CASE
        WHEN GCP.id IS NULL THEN 'Ausente no GCP'
        WHEN LCL.id IS NULL THEN 'Ausente no LCL'
        ELSE 'Ok'
      END AS verifica_id,
      GCP.code_gender as code_gender_gcp,
      LCL.code_gender as code_gender_lcl,
      CASE
        WHEN GCP.code_gender IS NULL THEN 'Null no GCP'
        WHEN LCL.code_gender IS NULL THEN 'Null no LCL'
        WHEN GCP.code_gender = LCL.code_gender THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_code_gender,

      GCP.flag_own_car as flag_own_car_gcp,
      LCL.flag_own_car as flag_own_car_lcl,
      CASE
        WHEN GCP.flag_own_car IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_own_car IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_own_car = LCL.flag_own_car THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_own_car,

      GCP.flag_own_realty as flag_own_realty_gcp,
      LCL.flag_own_realty as flag_own_realty_lcl,
      CASE
        WHEN GCP.flag_own_realty IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_own_realty IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_own_realty = LCL.flag_own_realty THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_own_realty,

      GCP.cnt_children as cnt_children_gcp,
      LCL.cnt_children as cnt_children_lcl,
      CASE
        WHEN GCP.cnt_children IS NULL THEN 'Null no GCP'
        WHEN LCL.cnt_children IS NULL THEN 'Null no LCL'
        WHEN GCP.cnt_children = LCL.cnt_children THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_cnt_children,
      
      GCP.amt_income_total as amt_income_total_gcp,
      LCL.amt_income_total as amt_income_total_lcl,
      CASE
        WHEN GCP.amt_income_total IS NULL THEN 'Null no GCP'
        WHEN LCL.amt_income_total IS NULL THEN 'Null no LCL'
        WHEN GCP.amt_income_total = LCL.amt_income_total THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_amt_income_total,

      GCP.name_income_type as name_income_type_gcp,
      LCL.name_income_type as name_income_type_lcl,
      CASE
        WHEN GCP.name_income_type IS NULL THEN 'Null no GCP'
        WHEN LCL.name_income_type IS NULL THEN 'Null no LCL'
        WHEN GCP.name_income_type = LCL.name_income_type THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_name_income_type,

      GCP.name_education_type as name_education_typ_gcp,
      LCL.name_education_type as name_education_typ_lcl,
      CASE
        WHEN GCP.name_education_type IS NULL THEN 'Null no GCP'
        WHEN LCL.name_education_type IS NULL THEN 'Null no LCL'
        WHEN GCP.name_education_type = LCL.name_education_type THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_name_education_type,

      GCP.name_family_status as name_family_status_gcp,
      LCL.name_family_status as name_family_status_lcl,
      CASE
        WHEN GCP.name_family_status IS NULL THEN 'Null no GCP'
        WHEN LCL.name_family_status IS NULL THEN 'Null no LCL'
        WHEN GCP.name_family_status = LCL.name_family_status THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_name_family_status,

      GCP.name_housing_type as name_housing_type_gcp,
      LCL.name_housing_type as name_housing_type_lcl,
      CASE
        WHEN GCP.name_housing_type IS NULL THEN 'Null no GCP'
        WHEN LCL.name_housing_type IS NULL THEN 'Null no LCL'
        WHEN GCP.name_housing_type = LCL.name_housing_type THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_name_housing_type,

      GCP.days_birth as days_birth_gcp,
      LCL.days_birth as days_birth_lcl,
      CASE
        WHEN GCP.days_birth IS NULL THEN 'Null no GCP'
        WHEN LCL.days_birth IS NULL THEN 'Null no LCL'
        WHEN GCP.days_birth = LCL.days_birth THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_days_birth,

      GCP.days_employed as days_employed_gcp,
      LCL.days_employed as days_employed_lcl,
      CASE
        WHEN GCP.days_employed IS NULL THEN 'Null no GCP'
        WHEN LCL.days_employed IS NULL THEN 'Null no LCL'
        WHEN GCP.days_employed = LCL.days_employed THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_days_employed,

      GCP.flag_mobil as flag_mobil_gcp,
      LCL.flag_mobil as flag_mobil_lcl,
      CASE
        WHEN GCP.flag_mobil IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_mobil IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_mobil = LCL.flag_mobil THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_mobil,

      GCP.flag_work_phone as flag_work_phone_gcp,
      LCL.flag_work_phone as flag_work_phone_lcl,
      CASE
        WHEN GCP.flag_work_phone IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_work_phone IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_work_phone = LCL.flag_work_phone THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_work_phone,

      GCP.flag_phone as flag_phone_gcp,
      LCL.flag_phone as flag_phone_lcl,
      CASE
        WHEN GCP.flag_phone IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_phone IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_phone = LCL.flag_phone THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_phone,

      GCP.flag_email as flag_email_gcp,
      LCL.flag_email as flag_email_lcl,
      CASE
        WHEN GCP.flag_email IS NULL THEN 'Null no GCP'
        WHEN LCL.flag_email IS NULL THEN 'Null no LCL'
        WHEN GCP.flag_email = LCL.flag_email THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_flag_email,

      GCP.occupation_type as occupation_type_gcp,
      LCL.occupation_type as occupation_type_lcl,
      CASE
        WHEN GCP.occupation_type IS NULL THEN 'Null no GCP'
        WHEN LCL.occupation_type IS NULL THEN 'Null no LCL'
        WHEN GCP.occupation_type = LCL.occupation_type THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_occupation_type,

      GCP.cnt_fam_members as cnt_fam_members_gcp,
      LCL.cnt_fam_members as cnt_fam_members_lcl,
      CASE
        WHEN GCP.cnt_fam_members IS NULL THEN 'Null no GCP'
        WHEN LCL.cnt_fam_members IS NULL THEN 'Null no LCL'
        WHEN GCP.cnt_fam_members = LCL.cnt_fam_members THEN 'Ok'
        ELSE 'N-Ok'
      END AS verifica_cnt_fam_members

    FROM tmp_application_record_gcp_trat GCP
    FULL OUTER JOIN tmp_application_record_local_trat LCL
      ON GCP.ID=LCL.ID
""";

EXECUTE IMMEDIATE """
  CREATE OR REPLACE TABLE `casegrupoboticario.dev_zone.tb_validation` AS
    SELECT 
      *,
      CASE
        WHEN verifica_id !='Ok' OR verifica_code_gender !='Ok' OR verifica_flag_own_car !='Ok' OR verifica_flag_own_realty !='Ok' OR verifica_cnt_children !='Ok' 
          OR verifica_amt_income_total !='Ok' OR verifica_name_income_type !='Ok' OR verifica_name_education_type !='Ok' OR verifica_name_family_status !='Ok' OR verifica_name_housing_type !='Ok'
          OR verifica_days_birth !='Ok' OR verifica_days_employed !='Ok' OR verifica_flag_mobil !='Ok' OR verifica_flag_work_phone !='Ok' OR verifica_flag_phone !='Ok' 
          OR verifica_flag_email !='Ok' OR verifica_occupation_type !='Ok' OR verifica_cnt_fam_members !='Ok' THEN '1 - Contém inconsistências'
        ELSE '2 - Não contém inconsistências' 
      END AS validacao_geral,
      CASE
        WHEN verifica_id !='Ok'THEN CONCAT('verifica_id: ', verifica_id) 
        WHEN verifica_code_gender !='Ok'THEN CONCAT('verifica_code_gender: ', verifica_code_gender)
        WHEN verifica_flag_own_car !='Ok'THEN CONCAT('verifica_flag_own_car: ', verifica_flag_own_car) 
        WHEN verifica_flag_own_realty !='Ok'THEN CONCAT('verifica_flag_own_realty: ', verifica_flag_own_realty) 
        WHEN verifica_cnt_children !='Ok'THEN CONCAT('verifica_cnt_children: ', verifica_cnt_children) 
        WHEN verifica_amt_income_total !='Ok'THEN CONCAT('verifica_amt_income_total: ', verifica_amt_income_total) 
        WHEN verifica_name_income_type !='Ok'THEN CONCAT('verifica_name_income_type: ', verifica_name_income_type) 
        WHEN verifica_name_education_type !='Ok'THEN CONCAT('verifica_name_education_type: ', verifica_name_education_type) 
        WHEN verifica_name_family_status !='Ok'THEN CONCAT('verifica_name_family_status: ', verifica_name_family_status) 
        WHEN verifica_name_housing_type !='Ok'THEN CONCAT('verifica_name_housing_type: ', verifica_name_housing_type) 
        WHEN verifica_days_birth !='Ok'THEN CONCAT('verifica_days_birth: ', verifica_days_birth) 
        WHEN verifica_days_employed !='Ok'THEN CONCAT('verifica_days_employed: ', verifica_days_employed) 
        WHEN verifica_flag_mobil !='Ok'THEN CONCAT('verifica_flag_mobil: ', verifica_flag_mobil)
        WHEN verifica_flag_work_phone !='Ok'THEN CONCAT('verifica_flag_work_phone: ', verifica_flag_work_phone)
        WHEN verifica_flag_phone !='Ok'THEN CONCAT('verifica_flag_phone: ', verifica_flag_phone)
        WHEN verifica_flag_email !='Ok'THEN CONCAT('verifica_flag_email: ', verifica_flag_email)
        WHEN verifica_occupation_type !='Ok'THEN CONCAT('verifica_occupation_type: ', verifica_occupation_type)
        WHEN verifica_cnt_fam_members !='Ok'THEN CONCAT('verifica_cnt_fam_members: ', verifica_cnt_fam_members)
        ELSE 'Ok' 
      END AS validacao_faseada
    FROM tmp_tb_validation
""";


EXECUTE IMMEDIATE """
  CREATE OR REPLACE TABLE `casegrupoboticario.dev_zone.tb_validation_errors` AS
    SELECT 
      *
    FROM `casegrupoboticario.dev_zone.tb_validation`
    WHERE  validacao_geral='1 - Contém inconsistências'
""";

END;
