# Importando as bibliotecas que vamos usar nesse exemplo
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
# Definindo alguns argumentos básicos
default_args = {
	'owner': 'rodrigo_kavabata',
	'depends_on_past': False,
	'start_date': datetime(2023, 3, 10),
	'retries': 0,
	}
# Nomeando a DAG e definindo quando ela vai ser executada (você pode usar argumentos em Crontab também caso queira que a DAG execute por exemplo todos os dias as 8 da manhã)
with DAG(
	'dag_projeto_final',
	schedule_interval="*/30 * * * *",
	catchup=False,
	default_args=default_args
	) as dag:
# Definindo as tarefas que a DAG vai executar, nesse caso a execução de dois programas Python, chamando sua execução por comandos bash
		t1 = BashOperator(
			task_id='gera_parquet',
			bash_command="""
			cd ../../../home/rodrigo/airflow/dags/projeto/
			python3 projeto_final.py
			""")
# Definindo o padrão de execução, nesse caso executamos t1 e depois t2
		t1
