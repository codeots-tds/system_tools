import psycopg2
import pandas as pd
import io
import numpy as np

"""loading data with pandas, and enter the path to the data"""
labor_df = pd.read_csv('')

"""To avoid data NaN issues we'll run the line below to see which columns don't have any
missing values and just look at the first few."""
print(labor_df.isna().any(), 'checking cols for missing values/nan')
labor_df = labor_df.iloc[:, :4]

"""creating conn object"""
conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='postgres',
    user='postgres',
    password='postgres'
)

"""creating cursor object"""
cur = conn.cursor()


"""set schema"""
create_job_table = """
CREATE TABLE IF NOT EXISTS job_data_table(
    job_num INT, 
    doc_num INT, 
    boro VARCHAR(50), 
    house_num VARCHAR(50)
)"""


"""insert data query"""
insert_table_query = """COPY {job_data_table} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER '\t')"""


"""dropping table query"""
drop_data_table = """
DROP TABLE IF EXISTS job_data_table
"""

"""Create table function"""
def create_table(conn, cur, sql_st):
    try:
        cur.execute(sql_st)
        print(f"""Table {sql_st} was successfully created!""")
        conn.commit()
    except psycopg2.Error as e:
        print("Error: Issue creating table!")
        print(e)

"""insert data function"""
def insert_data(conn, cur, df, sql_st):
    cols = ','.join(df.columns)
    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, sep='\t')
        buffer.seek(0)

        cur.copy_from(buffer, 'job_data_table', sep='\t')
        conn.commit()
    except psycopg2.Error as e:
        print("Error: Couldn't load data into postgres")
        print(e)
    else:
        return "Data was loaded!"
    
tables = [create_job_table]

    
if __name__ == "__main__":
    cur.execute(drop_data_table)
    for table_st in tables:
         create_table(conn = conn, cur = cur, sql_st = table_st)

    insert_data(conn = conn, cur = cur, df = labor_df, sql_st = insert_table_query)
    pass