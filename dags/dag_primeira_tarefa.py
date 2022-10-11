import os
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

def quantidade_passageiro(df):
    df = df[["Sex", "Pclass", "PassengerId"]].groupby(["Sex", "Pclass"]).count()
    df.columns = ["Resultado"]
    df.insert(0, "exercicio", "Quantidade de passageiros por sexo e classe")
    df.to_csv("Quantidade de passageiros por sexo e classe.csv", sep=";", encoding="ISO-8859-1")
    return df

def preco_medio_tarifa(df):
    df = df[["Sex", "Pclass", "Fare"]].groupby(["Sex", "Pclass"]).mean()
    df.columns = ["Resultado"]
    df.insert(0, "exercicio", "Preço médio da tarifa pago por sexo e classe")
    df.to_csv("Preço médio da tarifa pago por sexo e classe.csv", sep=";", encoding="ISO-8859-1")
    return df

def total_sibsp_parch(df):
    df['SibSp + Parch'] = df["SibSp"] + df["Parch"]
    df = df[["Sex", "Pclass", "SibSp + Parch"]].groupby(["Sex", "Pclass"]).sum()
    df.columns = ["Resultado"]
    df.insert(0, "exercicio", "Quantidade total de SibSp + Parch (tudo junto) por sexo e classe")
    df.to_csv("Quantidade total de SibSp + Parch (tudo junto) por sexo e classe.csv", sep=";", encoding="ISO-8859-1")
    return df

def main():
    df = pd.read_csv("https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv", header=0, sep=";", encoding="ISO-8859-1")
    df1 = quantidade_passageiro(df)
    df2 = preco_medio_tarifa(df)
    df3 = total_sibsp_parch(df)
    df_full = pd.concat([df1, df2, df3])
    df_full.to_csv("tabela_unica.csv", sep=";", encoding="ISO-8859-1")
    print(df_full)

dag = DAG(
            'calculo_indicadores', 
            description = 'Calcular indicadores',
            schedule_interval = None,
            start_date = datetime(2022, 10, 11), 
            catchup = False
        )

inicio = DummyOperator(task_id="início")
calcular_indicadores = PythonOperator(task_id = 'calcular_indicadores', python_callable = main, dag = dag)
trigger = TriggerDagRunOperator(task_id = "trigger", trigger_dag_id = "média_indicadores")
fim = DummyOperator(task_id="fim")

inicio >> calcular_indicadores >> trigger >> fim
