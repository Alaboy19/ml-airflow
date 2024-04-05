# dags/churn.py
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum
from airflow.decorators import dag, task

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_churn_dataset():
    
	#ваш код здесь #
    
    @task()
    def create_table() -> None:

        from sqlalchemy import MetaData, Table, Column, String, Integer, Float, inspect, String, DateTime, UniqueConstraint # дополните импорты необходимых типов колонок


        # ваш код здесь тоже #
        hook = PostgresHook('destination_db')
        db_conn = hook.get_sqlalchemy_engine()
        metadata = MetaData()
        users_churn = Table(
            'users_churn',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('customer_id', String),
            Column('begin_date', DateTime),
            Column('end_date',DateTime),
            Column('type',String),
            Column('paperless_billing',String),
            Column('payment_method',String),
            Column('monthly_charges',Float),
            Column('total_charges',Float),
            Column('internet_service',String),
            Column('online_security',String),
            Column('online_backup',String),
            Column('device_protection',String),
            Column('tech_support',String),
            Column('streaming_tv',String),
            Column('streaming_movies',String),
            Column('gender',String),
            Column('senior_citizen',Integer),
            Column('partner',String),
            Column('dependents',String),
            Column('multiple_lines',String),
            Column('target',Integer),
            UniqueConstraint('customer_id', name='unique_employee_constraint')
            ) 

        if not inspect(db_conn).has_table(users_churn.name): 
            metadata.create_all(db_conn) 
	
    @task()
    def extract(**kwargs):

        hook = PostgresHook('source_db')
        conn = hook.get_conn()
        sql = f"""
        select
            c.customer_id, c.begin_date, c.end_date, c.type, c.paperless_billing, c.payment_method, c.monthly_charges, c.total_charges,
            i.internet_service, i.online_security, i.online_backup, i.device_protection, i.tech_support, i.streaming_tv, i.streaming_movies,
            p.gender, p.senior_citizen, p.partner, p.dependents,
            ph.multiple_lines
        from contracts as c
        left join internet as i on i.customer_id = c.customer_id
        left join personal as p on p.customer_id = c.customer_id
        left join phone as ph on ph.customer_id = c.customer_id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(data: pd.DataFrame):
        data['target'] = (data['end_date'] != 'No').astype(int)
        data['end_date'].replace({'No': None}, inplace=True)
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="users_churn",
            replace=True,
            target_fields=data.columns.tolist(),
            replace_index=['customer_id'],
            rows=data.values.tolist())
        
    create_table()
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
prepare_churn_dataset()