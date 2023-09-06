from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import logging  #Para los registros de ejecución de procesos

default_args = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "email" : ["joel.rivera@leasein.pe"],
    "email_on_failure" : "joel.rivera@leasein.pe",
    "email_on_retry" : "joel.rivera@leasein.pe",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=5)
}


#Ahora si podemos definir las funciones a realizar
def scrape():
    logging.info("Comenzando scrapeo")
    
def procesamiento():
    logging.info("Procesamiento")

def save_data():
    logging.info("Guardando info")
    
    
    
#creamos los Flujos:
with DAG(
    'scrapping_data',
    default_args= default_args,
    description='Comenzando flujo de colección de data',
    schedule_interval= timedelta(days=1),
    start_date=days_ago(2),
    tags=["preparaciones"]   
) as dag:
    scrape_task =PythonOperator(task_id="scrape",python_callable=scrape)
    process_task = PythonOperator(task_id="procesamiento",python_callable=procesamiento)
    save_task = PythonOperator(task_id="save_data",python_callable=save_data)
    
    scrape_task >> process_task >> save_task
    