import pandas as pd
import time
from re import search
import numpy as np
import psycopg2
from pathlib import Path
from datetime import datetime
from io import StringIO

def db_connection():
    cnx = psycopg2.connect(database="data_analytics",
                            host="10.148.0.10",
                            user="datateam",
                            password="5ZpgMvoMzgmh_mD",
                            port="5432")
    return cnx


########## table to_test part ##########
def copy_from_stringio(conn, df, table):

    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

def get_data_frame(dfBase):
    list=[]
    matched_data_source_id=1
    for index,row in dfBase.iterrows():
        print(index)
        code=row['ID']
        value=(matched_data_source_id,code,current_dateTime,current_dateTime)
        list.append(value)
        df = pd.DataFrame(list)
    return(df)


        


if __name__ == "__main__":

    current_dateTime = datetime.now()

    ##################################################
    ################# to modify ######################
    ################################################## 
    start_time = time.time()
    base_file="C:/Users/narim/Desktop/POC/Data Processing/DPA-376/data sources/POC_Network_pharmacy_list.xlsx"
    dfBase = pd.read_excel(base_file,'Sheet1')
    print('1')
    conn=db_connection()
    print('2')
    get_df_start_time=time.time()
    df=get_data_frame(dfBase)
    get_df_end_time=time.time()
    get_df_execution_time = get_df_end_time - get_df_start_time
    print('get data frame execution time:',get_df_execution_time)
    print('3')
    table='test'
    copy_start_time=time.time()
    copy_from_stringio(conn, df, table)
    print('4')
    copy_end_time=time.time()
    copy_execution_time = copy_end_time - copy_start_time
    print('get data frame execution time:',copy_execution_time)
    end_time = time.time()
    execution_time = end_time - start_time
    print('duree de data import:',execution_time)