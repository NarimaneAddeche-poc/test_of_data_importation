import pandas as pd
import time
from re import search
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime

def db_connection():
    cnx = psycopg2.connect(database="data_analytics",
                            host="10.148.0.10",
                            user="datateam",
                            password="5ZpgMvoMzgmh_mD",
                            port="5432")
    return cnx

conn=db_connection()
cursor = conn.cursor()

########## table to_test part ##########
def test_insertion(data_values):
    sql=""" INSERT INTO test (matching_attempts_id,base_item_code,created_at,updated_at) VALUES (%s,%s,%s,%s)"""
    cursor.executemany(sql,data_values)
    #print(data_values)
    conn.commit()

def data_importation(base_file):
    dfBase = pd.read_excel(base_file,'Sheet1')
    print('in test table')
    data_values=[]
    matching_attempts_id=1
    for index,row in dfBase.iterrows():
        element=row['ID']
        list=(matching_attempts_id,element,current_dateTime,current_dateTime)
        data_values.append(list)
    test_insertion(data_values)

if __name__ == "__main__":

    current_dateTime = datetime.now()

    ##################################################
    ################# to modify ######################
    ################################################## 
    start_time = time.time()
    base_file="C:/Users/narim/Desktop/POC/Data Processing/DPA-376/data sources/POC_Network_pharmacy_list.xlsx"
    data_importation(base_file)
    end_time = time.time()
    execution_time = end_time - start_time
    print('duree de data import:',execution_time)