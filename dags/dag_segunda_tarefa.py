import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def main():
    df = pd.read_csv("tabela_unica.csv", sep=";", encoding="ISO-8859-1")
    df1 = df['Resultado'][df["exercicio"] == "Quantidade de passageiros por sexo e classe"].mean()
    df2 = df['Resultado'][df["exercicio"] == "Preço médio da tarifa pago por sexo e classe"].mean()
    df3 = df['Resultado'][df["exercicio"] == "Quantidade total de SibSp + Parch (tudo junto) por sexo e classe"].mean()
    print(f"Média da quantidade de passageiros por sexo e classe {df1}")
    print(f"Média do preço médio da tarifa pago por sexo e classe {df2}")
    print(f"Média da quantidade total de SibSp + Parch (tudo junto) por sexo e classe {df3}")


dag = DAG(
            'média_indicadores', 
            description = 'Calcular indicadores',
            schedule_interval = None,
            start_date = datetime(2022, 10, 11), 
            catchup = False
        )

inicio = DummyOperator(task_id="início")
media_indicadores = PythonOperator(task_id = 'média_indicadores', python_callable = main, dag = dag)
fim = DummyOperator(task_id="fim")

inicio >> media_indicadores >> fim
