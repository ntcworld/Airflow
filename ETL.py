import datetime
import pendulum
import os
import requests

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator



@dag(
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
# Table creation --> @Task get_data() --> @Task merge data --> Resister Etl() to DAG
def Etl():

    # create a destination table
    create_employees_table = MySqlOperator(
        task_id="create_employees_table",
        mysql_conn_id='company_db',
        sql="""
        CREATE TABLE IF NOT EXISTS employees(
            "Serial_Number" NUMERIC PRIMARY KEY,
            "Company_Name" VARCHAR(50),
            "Employee_Markme" VARCHAR(30),
            `Description` LONGTEXT,
            `Leave` INT 
        );
        """
    )

    # create temp table, This is a middle table to extract the increased data
    create_employees_temp_table = MySqlOperator(
        task_id="create_employees_temp_table",
        mysql_conn_id='company_db',
        sql="""
        DROP TABLE IF EXISTS employees_temp;
        CREATE TABLE IF NOT EXISTS employees(
            "Serial_Number" NUMERIC PRIMARY KEY,
            "Company_Name" VARCHAR(50),
            "Employee_Markme" VARCHAR(30),
            `Description` LONGTEXT,
            `Leave` INT 
        );
        """
    )

    # get_data() is a task what download a csv file on web using file stream, and load on a temp table with MysqlHook.
    @task
    def get_data():
        data_path = "/home/airflow/leegs/files/employees.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/pipeline_example.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        mysql_hook = MySqlHook(mysql_conn_id="company_db") # Mysql_hook
        conn = mysql_hook.get_conn()
        if conn:
            mysql_hook.bulk_load()
        else:
            print("It can't connect Mysql")
            extra_options="""FIELDS 
                TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
            LINES
                TERMINATED BY '\n'
                IGNORE 1 LINES
            (Serial_Number, Company_Name, Employee_Markme, Description, Leave)
            """

        mysql_hook.bulk_load_custom('employee', '/home/airflow/leegs/files/employees.csv', 'REPLACE', extra_options)
        conn.commit()

    # merge_data() is a task that load a data of temp-table to a destination table.
    @task
    def merge_data():
        query = """
            INSERT INTO employees
            SELect *
            FROM(
                SELECT DISTINCT *
                FROM employees_temp
            )
            ON DUPLICATE KEY UPDATE Serial_Number=Value(Serial_Number)
        """
        try:
            mysql_hook = MySqlHook(mysql_conn_id="company_db")
            conn = mysql_hook.get_conn()
            conn.send_query(query)
            conn.commit()
            return 0
        except Exception as e:
            return 1

    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()

dag=Etl()
