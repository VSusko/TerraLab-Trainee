from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Caminho do CSV dentro do container
CSV_PATH = "/opt/airflow/data/dados_processo_seletivo.csv"

# Task que realiza a leitura dos dados .csv
def leitura():
    # Leitura do CSV
    try:
        df = pd.read_csv(CSV_PATH, sep=',', encoding='utf-8')
    except Exception as e:
        print(f"DEBUG ERRO| Erro ao ler o CSV: {e}")

    print("DEBUG| Colunas do CSV:", df.columns.tolist())
    
    return df
    
def filtragem(ti):
    # Obtendo o data frame da task anterior
    df = ti.xcom_pull(task_ids='leitura')

    # Filtra todos os endereços com a cidade aracaju
    df = df[df['city'].str.upper() == 'ARACAJU']
    
    # Valida se nenhum registro foi encontrado
    if len(df) == 0:
        raise ValueError("Nenhum registro encontrado.")
    
    # Remove coluna indesejada
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    print(f"DEBUG| Numero de registros em Aracaju: {len(df)}")
    
    # Reseta índice    
    df = df.reset_index(drop=True)
    
    return df

def carregar_banco(ti):
    # Obtendo o data frame da task anterior
    df = ti.xcom_pull(task_ids='filtragem')
    
    # Estabelecendo conexao com o Postgrees
    hook = PostgresHook(postgres_conn_id='airflow_db')

    # Dropa e recria a tabela para evitar conflitos de colunas
    hook.run("DROP TABLE IF EXISTS enderecos_aracaju;")

    create_table_sql = """
    CREATE TABLE enderecos_aracaju (
        id SERIAL PRIMARY KEY,
        city VARCHAR,
        state VARCHAR,
        latitude FLOAT,
        longitude FLOAT,
        accuracy FLOAT,
        geoapi_id VARCHAR,
        date DATE
    );
    """
    hook.run(create_table_sql)

    # Inserindo o data frame no banco
    df.to_sql(
        name='enderecos_aracaju',
        con=hook.get_sqlalchemy_engine(),
        if_exists='append',
        index=False,
        method='multi'
    )

    print(f"DEUBG| {len(df)} registros de Aracaju inseridos no banco")
    print(f"DEUBG| SUCESSO")


with DAG(
    dag_id='etl_aracaju',
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etapa2', 'etl']
) as dag:
    
    leitura = PythonOperator(
        task_id='leitura',
        python_callable=leitura
    )
    
    filtragem = PythonOperator(
        task_id='filtragem',
        python_callable=filtragem
    )
    carregar_banco = PythonOperator(
        task_id='carregar_banco',
        python_callable=carregar_banco
    )
    
    leitura >> filtragem >> carregar_banco