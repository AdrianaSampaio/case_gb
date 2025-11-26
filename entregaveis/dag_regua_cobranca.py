from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import ShortCircuitOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable 
import pendulum
from datetime import timedelta
import os
import shutil
import json
import logging

# Configuração básica de log
log = logging.getLogger(__name__)

# =================================================================
# VARIÁVEIS DE CONFIGURAÇÃO (PARAMETRIZÁVEIS VIA AIRFLOW UI)
# =================================================================

try:
    # 1. Retries e Delay
    TENTATIVAS = int(Variable.get("regua_cobranca_retries", default_var=3))
    DELAY_MINUTOS = int(Variable.get("regua_cobranca_retry_delay_min", default_var=5))
    
    # 2. Dias de Envio ao Banco (ISO: 1=Segunda, 7=Domingo)
    DIAS_FLUXO_BANCO_RAW = Variable.get("regua_cobranca_dias_envio_banco", default_var="1,2,3,4,5")
    
    # Converte a variável (string separada por vírgulas ou JSON) para uma lista de inteiros
    if '[' in DIAS_FLUXO_BANCO_RAW:
         DIAS_FLUXO_BANCO = json.loads(DIAS_FLUXO_BANCO_RAW)
    else:
         DIAS_FLUXO_BANCO = [int(d.strip()) for d in DIAS_FLUXO_BANCO_RAW.split(',')]
         
except Exception as e:
    log.warning(f"Erro ao carregar variáveis Airflow: {e}. Usando valores padrão.")
    TENTATIVAS = 3
    DELAY_MINUTOS = 5
    DIAS_FLUXO_BANCO = [1, 2, 3, 4, 5]


# =================================================================
# CONSTANTES DE DIRETÓRIOS E ARQUIVOS
# =================================================================

NOME_ARQUIVO = "pagamentos_d-1.csv"
DIR_ORIGEM = "/opt/airflow/data/entrada"
DIR_PROCESSADOS = "/opt/airflow/data/logs_arquivos_processados"
DIR_CARGA_FINAL = "/opt/airflow/data/carga_finalizada"
DIR_ERROS = "/opt/airflow/data/erros"


# =================================================================
# FUNÇÕES DE SUPORTE
# =================================================================

def listar_arquivos_csv(caminho):
    """Lista o caminho completo de todos os arquivos CSV em um diretório."""
    return [
        os.path.join(caminho, f)
        for f in os.listdir(caminho)
        if os.path.isfile(os.path.join(caminho, f)) and f.endswith(".csv")
    ]

def mover(src, dst, filename):
    """Cria o destino se não existir e move um arquivo específico."""
    os.makedirs(dst, exist_ok=True)
    caminho_src = os.path.join(src, filename)
    shutil.move(caminho_src, dst)
    log.info(f"Arquivo {filename} movido de {src} para {dst}.")

def get_arquivo_path(base_dir=DIR_ORIGEM, filename=NOME_ARQUIVO):
    """Retorna o caminho completo de um arquivo esperado."""
    return os.path.join(base_dir, filename)


# =================================================================
# ARGUMENTOS E DEFINIÇÃO DA DAG
# =================================================================

default_args = {
    "owner": "cobrança",
    "retries": TENTATIVAS, # Parametrizado
    "retry_delay": timedelta(minutes=DELAY_MINUTOS), # Parametrizado
}

with DAG(
    dag_id="dag_regua_cobranca_parametrizavel",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="*/5 * * * *", 
    catchup=False,
    tags=["cobranca", "critico", "parametrizavel"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. VERIFICAÇÃO E SAÍDA (A CADA 5 MIN)
    def check_file_and_exit():
        """Verifica se o arquivo específico do ciclo existe e retorna False para Short-Circuit se não."""
        if os.path.exists(get_arquivo_path(DIR_ORIGEM)):
            return True
        log.info("Arquivo de entrada não encontrado. Encerrando o ciclo (Short-Circuit).")
        return False

    verificar_arquivo = ShortCircuitOperator(
        task_id="verificar_e_sair_se_vazio",
        python_callable=check_file_and_exit,
    )

    # 2. PROCESSAMENTO E ACUMULAÇÃO (MOVIMENTO PARA DIR_PROCESSADOS)
    @task(task_id="processar_e_acumular_pendentes")
    def processar_dados_task():
        """Processa o arquivo e o move de DIR_ORIGEM para DIR_PROCESSADOS."""
        log.info(f"Iniciando processamento de {get_arquivo_path(DIR_ORIGEM)}...")
        mover(DIR_ORIGEM, DIR_PROCESSADOS, NOME_ARQUIVO)
        return NOME_ARQUIVO 

    # Callback de falha (após 3 retries)
    def on_failure_move_to_error_alert(context):
        """Move o arquivo para DIR_ERROS e loga um alerta crítico."""
        if os.path.exists(get_arquivo_path(DIR_ORIGEM)):
            mover(DIR_ORIGEM, DIR_ERROS, NOME_ARQUIVO)
            log.error(f"ALERTA CRÍTICO: Falha persistente na task {context['task_instance'].task_id}! Arquivo movido para: {DIR_ERROS}")

    processamento = processar_dados_task.override(
        on_failure_callback=on_failure_move_to_error_alert
    )()
    
    # 3. BRANCHING (DECISÃO DE LOTE OU PARADA)
    def branch_dia_util():
        """Direciona para o fluxo de carga em lote se o dia estiver parametrizado para envio ao banco."""
        tz = pendulum.timezone("America/Sao_Paulo")
        hoje = pendulum.now(tz)
        
        # Usa a lista de dias parametrizada via Airflow Variable
        dia_de_carga = hoje.isoweekday() in DIAS_FLUXO_BANCO
        
        # Fim de semana (Dias NÃO configurados): Para e acumula em DIR_PROCESSADOS
        return "enviar_para_banco_task" if dia_de_carga else "end"

    branch = BranchPythonOperator(
        task_id="verificar_dia_util",
        python_callable=branch_dia_util
    )

    # 4A. FLUXO DIA ÚTIL (OPERAÇÃO EM LOTE)
    @task(task_id="enviar_para_banco_task")
    def enviar_para_banco_task():
        """Lê TODOS os arquivos de DIR_PROCESSADOS e carrega no Banco de Produção."""
        arquivos_pendentes = listar_arquivos_csv(DIR_PROCESSADOS)
        
        if not arquivos_pendentes:
            log.info("Nenhum arquivo pendente de carga no banco em DIR_PROCESSADOS.")
            return True

        log.info(f"--- CARGA EM LOTE INICIADA: {len(arquivos_pendentes)} ARQUIVOS ---")
        for arq_path in arquivos_pendentes:
            log.info(f"Carregando {os.path.basename(arq_path)} no Banco de Produção.")
            # Lógica de conexão/INSERT/UPSERT aqui
            
    @task(task_id="mover_para_carga_final")
    def mover_para_carga_final():
        """Move TODOS os arquivos de DIR_PROCESSADOS para DIR_CARGA_FINAL."""
        arquivos_a_mover = listar_arquivos_csv(DIR_PROCESSADOS)
        
        if not arquivos_a_mover:
            return

        log.info(f"Movendo {len(arquivos_a_mover)} arquivos para DIR_CARGA_FINAL.")
        os.makedirs(DIR_CARGA_FINAL, exist_ok=True)
        
        # Move cada arquivo individualmente usando shutil.move (seguro)
        for arq_path in arquivos_a_mover:
            filename = os.path.basename(arq_path)
            shutil.move(arq_path, os.path.join(DIR_CARGA_FINAL, filename))

    # =================================================================
    # DEFINIÇÃO DO FLUXO (Dependencies)
    # =================================================================
    
    start >> verificar_arquivo >> processamento >> branch
    
    # Caminho Dia Útil: Processados -> Banco (Lote) -> Carga Final (Lote) -> END
    branch >> enviar_para_banco_task() >> mover_para_carga_final() >> end
    
    # Caminho Fim de Semana: Para e Acumula (Branch aponta para 'end')
    branch >> end