import MySQLdb
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
import csv
import pymysql
import pandas as pd
import numpy as np

# Backwards compatibility of pymysql to mysqldb
pymysql.install_as_MySQLdb()

# Importing MySQLdb now
dag = DAG(
    'Data_like_Pipeline',
    description='ELT pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)


def handleStates(df):
    df.State = df.State.str.strip()  # remove whitespaces
    states = df['State'].unique()
    for state in states:          # loop for each state
        if state.isupper():      # take only the states uppercased
            # replace the ones that are lowercase or mixed with the uppercased one
            df.loc[df.State.str.upper() == state, 'State'] = state


def preprocess_data():
    df_cars = pd.read_csv("tc20171021.csv", on_bad_lines='skip')
    df_cars.drop('Id', axis=1, inplace=True)
    df = pd.read_csv("true_car_listings.csv")
    concat_cars = pd.concat([df, df_cars])
    concat_cars.drop_duplicates(subset='Vin', keep='first', inplace=True)
    handleStates(concat_cars)
    concat_cars['Make'] = concat_cars['Make'].str.upper()
    concat_cars['Model'] = concat_cars['Model'].str.upper()
    concat_cars.to_csv('/data/cars/cleancars.csv')


preprocess_data_task = PythonOperator(
    task_id='preprocess_data_task',
    python_callable=preprocess_data,
    dag=dag,
)

create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='cars_data',
    sql='/sql/create_cars_table.sql',
    dag=dag,
)


def csvToSql():

    # Attempt connection to a database
    try:
        dbconnect = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='databasepwd',
            db='mydb'
        )
    except:
        print('Can\'t connect.')

    # Define a cursor iterator object to function and to traverse the database.
    cursor = dbconnect.cursor()
    # Open and read from the .csv file
    with open('..data/cars/cleancars.csv') as csv_file:

        # Assign the .csv data that will be iterated by the cursor.
        csv_data = csv.reader(csv_file)

        # Insert data using SQL statements and Python
        for row in csv_data:
            cursor.execute(
                'INSERT INTO Cars(Price,Year,Mileage,city,state,Vin,Make,Model) VALUES("%f", "%i", "%i", "%s", "%s", "%s", "%s", "%s", "%s")'
    # Commit the changes
    dbconnect.commit()

    '''
    # Print all rows - FOR DEBUGGING ONLY
    cursor.execute("SELECT * FROM rogobenDB3")
    rows = cursor.fetchall()

    print(cursor.rowcount)
    for row in rows:
        print(row)
    '''

    # Close the connection
    cursor.close()

    # Confirm completion
    return 'Read .csv and written to the MySQL database'


load_data_task=BashOperator(
    task_id='load_order_data',
    python_callable=csvToSql,
    dag=dag,
)

preprocess_data_task >> create_table >> load_data_task
