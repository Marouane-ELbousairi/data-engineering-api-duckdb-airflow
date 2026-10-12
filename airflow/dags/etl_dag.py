import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import requests
import duckdb

from airflow import DAG
from airflow.operators.python import PythonOperator

# Configuration des chemins d'accès
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = "/opt/airflow/ecommerce_data.duckdb"
LOG_FILE = os.path.join(os.path.dirname(BASE_DIR), "etl_creation.log")

# Configuration du logger local
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------------------------------------------------------
# Fonctions de l'ETL
# ------------------------------------------------------------------

def extract_api_data(**kwargs):
    """Étape 1 : Extraction des données depuis l'API REST."""
    url = "https://fakestoreapi.com/products"
    logging.info(f"Extraction des données depuis l'API : {url}")
    
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    
    products = response.json()
    logging.info(f"Nombre de produits extraits : {len(products)}")
    
    # Transmission des données brutes à la tâche suivante via XCom
    kwargs['ti'].xcom_push(key='raw_products', value=products)


def transform_data(**kwargs):
    """Étape 2 : Nettoyage et transformation avec Pandas."""
    ti = kwargs['ti']
    raw_products = ti.xcom_pull(key='raw_products', task_ids='extract_data')
    
    if not raw_products:
        raise ValueError("Aucune donnée reçue de l'étape d'extraction.")
    
    # Conversion en DataFrame Pandas
    df = pd.DataFrame(raw_products)
    
    # Nettoyage et extraction de la note (rating)
    df['rating_rate'] = df['rating'].apply(lambda x: x.get('rate') if isinstance(x, dict) else None)
    df['rating_count'] = df['rating'].apply(lambda x: x.get('count') if isinstance(x, dict) else None)
    
    # Sélection et renommage des colonnes
    df = df[['id', 'title', 'price', 'category', 'rating_rate', 'rating_count']]
    df['extracted_at'] = datetime.now().isoformat()
    
    logging.info(f"Transformation réussie. Colonnes traitées : {list(df.columns)}")
    
    # Transmission du DataFrame nettoyé au format dict
    ti.xcom_push(key='cleaned_products', value=df.to_dict(orient='records'))


def load_to_duckdb(**kwargs):
    """Étape 3 : Chargement des données dans la base DuckDB."""
    ti = kwargs['ti']
    cleaned_products = ti.xcom_pull(key='cleaned_products', task_ids='transform_data')
    
    df = pd.DataFrame(cleaned_products)
    
    logging.info(f"Connexion à la base DuckDB : {DB_PATH}")
    conn = duckdb.connect(DB_PATH)
    
    # Création de la table si elle n'existe pas
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            title VARCHAR,
            price DOUBLE,
            category VARCHAR,
            rating_rate DOUBLE,
            rating_count INTEGER,
            extracted_at TIMESTAMP
        )
    """)
    
    # Insertion / Mise à jour dans DuckDB
    conn.execute("INSERT OR REPLACE INTO products SELECT * FROM df")
    
    count = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    conn.close()
    
    logging.info(f"Chargement terminé avec succès. Nombre total de produits dans DuckDB : {count}")

# ------------------------------------------------------------------
# Définition du DAG Airflow
# ------------------------------------------------------------------

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='ecommerce_api_to_duckdb',
    default_args=default_args,
    description='Pipeline ETL de récupération des produits API vers DuckDB',
    schedule_interval='@daily',
    catchup=False,
    tags=['ecommerce', 'duckdb', 'api'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_api_data,
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    task_load = PythonOperator(
        task_id='load_to_duckdb',
        python_callable=load_to_duckdb,
    )

    # Définition des dépendances (Ordre d'exécution)
    task_extract >> task_transform >> task_load