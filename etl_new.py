import configparser
import psycopg2

import importlib
import sql_S3_to_staging_etl
importlib.reload(sql_S3_to_staging_etl)

from sql_S3_to_staging_etl import *

from sys import argv
script,var1 = argv

def execute_etl(cur, conn):
    print(f"etl function triggered:  {var1}")
    cur.execute(var1)
    conn.commit()

def main():
    
    print(f"python script is:  {script}")
    
    config = configparser.ConfigParser()
    config.read('/Users/chrisstephenson/repos/capstone_proj.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    execute_etl(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()