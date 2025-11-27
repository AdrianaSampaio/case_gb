# contador registros totais e ids distintos (auxilia na verificação de duplicidade de ids além do conheicmento de totais de registros, que também pode ser verificado olhando os detalhes da tabela)
select 
  count(*) as qtd_linhas,
  count(distinct id) as qtd_id
from `casegrupoboticario.dev_zone.tb_validation`;


# contador registros sem ou com incosistencias 
select 
  validacao_geral,
  count(*) as qtd
from `casegrupoboticario.dev_zone.tb_validation`
group by all 
order by validacao_geral;


# contador registros por incosistencias
select 
  validacao_geral,
  validacao_faseada,
  count(*) as qtd
from `casegrupoboticario.dev_zone.tb_validation_errors`
group by all 
order by validacao_geral, qtd desc;

#abertura dos casos relativos ao flag_work_phone
select 
  verifica_flag_work_phone,
  flag_work_phone_gcp,
  flag_work_phone_lcl,
  count(*) as qtd
from `casegrupoboticario.dev_zone.tb_validation_errors`
where validacao_faseada like '%verifica_flag_work_phone: Null no GCP%'
and validacao_faseada not like 'verifica_id: Ausente no GCP,%'
group by all 
order by  qtd desc;

#abertura dos casos relativos ao verifica_occupation_type
select 
  verifica_occupation_type,
  occupation_type_gcp,
  occupation_type_lcl,
  count(*) as qtd
from `casegrupoboticario.dev_zone.tb_validation_errors`
where validacao_faseada like '%verifica_occupation_type: Null no LCL%'
and validacao_faseada not like 'verifica_id: Ausente no GCP,%'
group by all 
order by  qtd desc;