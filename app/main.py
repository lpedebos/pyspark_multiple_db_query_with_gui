import json
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import time
import datetime
import psycopg2
import os

def create_spark_session():
    return SparkSession.builder \
        .appName("MultiPostgresConnection") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

def read_query_from_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read()  # Remove blank spaces
        query = query.rstrip(';') # Remove ";" at the end of file, if exists
    return query


def read_multiple_queries_from_file(file_path: str) -> list:
    with open(file_path, 'r') as file:
        content = file.read()
    # Divide por ';' e remove espaços / linhas vazias
    queries = [q.strip() for q in content.split(";") if q.strip()]
    return queries


def execute_multiple_queries(spark, url, database_name, queries, user, password):
    df = None

    for q in queries:
        if not q.strip():
            continue
        
        if is_select(q):
            #print(q)
            #print('executando query via spark')
            # Executa SELECTs via Spark
            df = read_from_postgres(
                spark=spark,
                url=url,
                database_name=database_name,
                query=q,
                user=user,
                password=password
            )
        else:
            #print(q)
            #print('excutando DDL/DML via psycopg2')
            # Executa DDL/DML via psycopg2
            execute_ddl(url, user, password, q)

    if df is None:
        raise ValueError("Nenhuma query SELECT ou WITH encontrada no arquivo SQL.")
    
    return df

def read_from_postgres(spark: SparkSession, url: str, database_name: str, query: str, user: str, password: str) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("database_name", database_name) \
        .option("query", query) \
        .option("user", user) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def load_config(file_path: str):
    with open(file_path, 'r') as file:
        return json.load(file)

def execute_query_on_combined_df(spark: SparkSession, combined_df: DataFrame, query: str) -> DataFrame:
    combined_df.createOrReplaceTempView("combined_table") # set name for combined dataframe
    return spark.sql(query)

def save_df_to_csv(df: DataFrame, file_path: str, sep: str = ";"):
    with open(file_path, 'w') as file:
        header = sep.join(df.columns)
        file.write(header + "\n")
        for row in df.collect():
            line = sep.join([str(item) for item in row])
            file.write(line + "\n")


def is_select(query: str) -> bool:
    q = query.strip().lower()
    return q.startswith("select") or q.startswith("with")




def execute_ddl(url, user, password, query):
    # Ajusta a URL para padrão psycopg2
    pg_url = normalize_jdbc_url(url)

    conn = psycopg2.connect(pg_url, user=user, password=password)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def normalize_jdbc_url(url: str) -> str:
    # Exemplo: jdbc:postgresql://host:port/db -> postgresql://host:port/db
    if url.startswith("jdbc:"):
        return url.replace("jdbc:", "")
    return url



# Usage example
if __name__ == "__main__":
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Creating spark session 🔥")
    spark = create_spark_session()
    #time.sleep(0.5)
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Loading configurations for database connection 🚩")
    config = load_config('config.json')
    #time.sleep(0.5)
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Loading queries 🤔")
    queries = read_multiple_queries_from_file('first_query.sql')
    second_query = read_query_from_file('second_query.sql')
    #time.sleep(0.5)
    
    dataframes = []
    #database_name = 1
    
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Executing queries on database ⌛")
    #time.sleep(0.5)
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - There are {len(config['databases'])} databases to process 📚")
    for db in config['databases']:
        df = execute_multiple_queries(
            spark=spark,
            url=db['url'],
            database_name=db['database_name'], 
            queries=queries,
            user=db['user'],
            password=db['password']
        )
        print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Executing query on database {db['database_name']} ⌛")
        #time.sleep(0.5)
        dataframes.append(df)
        print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Query on database {db['database_name']} executed ✅")
        #time.sleep(0.5)
        #database_name += 1
    
    # Union of dataframes
    combined_df = dataframes[0]
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Combining results from all databases in a dataframe ♾️")
    #time.sleep(0.5)
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)
    
    # Execution of the second query on combined dataframe
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Executing second query on combined dataframe 🌀")
    #time.sleep(0.5)
    result_df = execute_query_on_combined_df(spark, combined_df, second_query)
    
    # Save results to a CSV file
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Saving results to file 💾")
    #time.sleep(0.5)
    OUTPUT_FILE = os.path.join(os.getcwd(), "results.csv")
    save_df_to_csv(result_df, OUTPUT_FILE)

    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Results saved to {OUTPUT_FILE} 🚀")
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Click on the button bellow to download results 🔽") 