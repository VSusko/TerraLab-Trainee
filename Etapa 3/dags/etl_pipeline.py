from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

CSV_PATH = "/opt/airflow/data/dados_processo_seletivo.csv"


SERGIPE = {
    "lat_min": -11.6,
    "lat_max": -9.5,
    "lon_min": -38.3,
    "lon_max": -36.4
}

# Funcao auxiliar que verifica se o ponto está dentro da unidade federativa (nesse caso, sergipe)
def ponto_dentro_uf(lat, lon, bbox):
    return (
        bbox["lat_min"] <= lat <= bbox["lat_max"] and
        bbox["lon_min"] <= lon <= bbox["lon_max"]
    )

# Extracao dos enderecos do .csv
def extracao_dados():
    # Extração
    df = pd.read_csv(CSV_PATH, sep=',', encoding='utf-8', low_memory=False)

    print(f"DEBUG| Total de registros lidos: {len(df)}")   
    
    return df

# Realizando a filtragem dos enderecos extraidos
def transformacoes(ti):
    # Obtendo o objeto da task anterior
    df = ti.xcom_pull(task_ids='extracao_dados')
    
    # Transformação 1 — remove OpenRouteService
    df = df[~df['geoapi_id'].str.contains('openrouteservice', case=False, na=False)]
    
    print(f"DEBUG| Após remover OpenRouteService: {len(df)}")

    # Transformação 2 — validação espacial (UF)
    df = df[
        df.apply(
            lambda row: ponto_dentro_uf(
                row['latitude'], row['longitude'], SERGIPE
            ),
            axis=1
        )
    ]
    print(f"DEBUG| Após validação espacial: {len(df)}")
    
    # Limpeza
    if 'Unnamed: 0' in df.columns:
        df = df.drop(columns=['Unnamed: 0'])

    df = df.reset_index(drop=True)
    
    return df
    
# Task que verifica se os enderecos obtidos na transformacao sao de fato validos
def validacao(ti):
    # Obtendo o objeto da task anterior
    df = ti.xcom_pull(task_ids='transformacoes')
    
    # Validando se o dataframe esta vazio
    if df is None or df.empty:
        raise ValueError("Dataset vazio")
    
    # Validando as latitudes do dataframe
    if not df['latitude'].between(-90, 90).all():
        raise ValueError("Latitudes fora do intervalo")

    # Validando as longitudes do dataframe
    if not df['longitude'].between(-180, 180).all():
        raise ValueError("Longitudes fora do intervalo")
    
    if df['state'].all() != 'SE':
        raise ValueError("Existem registros fora de Sergipe")
    
    total = len(df)
    print(f"DEBUG| Total final de registros válidos: {total}")    
    
    if total < 1:
        raise ValueError("Nenhum registro válido encontrado")
    
    
    print(f"DEBUG| Validação Completa")    
    return df

# Task que insere o resultado da validacao no banco de dados
def carregar_banco(ti):
    # Obtendo o objeto da task anterior
    df = ti.xcom_pull(task_ids='validacao')
    
    # Conexao com o Postgrees
    hook = PostgresHook(postgres_conn_id='airflow_db')

    hook.run("DROP TABLE IF EXISTS enderecos_tratados;")

    create_table_sql = """
    CREATE TABLE enderecos_tratados (
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
        name='enderecos_tratados',
        con=hook.get_sqlalchemy_engine(),
        if_exists='append',
        index=False,
        method='multi'
    )

    print("DEBUG| Banco de dados carregado")
    print("DEBUG| SUCESSO")

# DAG dessa etapa: extracao -> transformacao -> validacao -> insercao no banco
with DAG(
    dag_id='etl_pipeline_real',
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,
    catchup=False,
    tags=['etapa3', 'pipeline_real']
) as dag:

    extracao_dados = PythonOperator(
        task_id='extracao_dados',
        python_callable=extracao_dados
    )
    transformacoes = PythonOperator(
        task_id='transformacoes',
        python_callable=transformacoes
    )
    validacao = PythonOperator(
        task_id='validacao',
        python_callable=validacao
    )
    carregar_banco = PythonOperator(
        task_id='carregar_banco',
        python_callable=carregar_banco
    )
    
    extracao_dados >> transformacoes >> validacao >> carregar_banco    