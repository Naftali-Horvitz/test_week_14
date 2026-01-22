import mysql.connector
import os
import pandas as pd


db_password = os.getenv('MYSQL_ROOT_PASSWORD')
db_name = os.getenv('MYSQL_DATABASE')

def get_conn():
    return mysql.connector.connect(
    password=db_password,
    database=db_name
)

def create_table(conn):
    cursor = conn.cursor()
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weapons (
            id INT AUTO_INCREMENT PRIMARY KEY,
            weapon_id VARCHAR(255),
            weapon_name: VARCHAR(255),
            weapon_type: VARCHAR(255), 
            range_km: INT
            weight_kg: FLOAT
            manufacturer: VARCHAR(255),
            origin_country: VARCHAR(255),
            storage_location: VARCHAR(255), 
            year_estimated: INT,
            risk_level
        )
        """
        cursor.execute(create_table_query)
        conn.commit() 
        print("Table created successfully!")
    except Exception as e:
        raise e


def insert_data(data: list[dict], conn):
    insert_data_query = """
    INSERT INTO weapons (weapon_id, weapon_name, weapon_type, range_km, 
    weight_kg, manufacturer, origin_country, storage_location, year_estimated, risk_level)
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    data = data
    cursor = conn.cursor()
    for i in data:
        cursor.executemany(insert_data_query, tuple(i.values()))
    return {
    "status": "success",
    "inserted_records": cursor.rowcount
    }

    
