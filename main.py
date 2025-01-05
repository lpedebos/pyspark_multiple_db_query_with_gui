import json
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import time
import datetime

def create_spark_session():
    return SparkSession.builder \
        .appName("MultiPostgresConnection") \
        .config("spark.jars", "postgresql-42.7.4.jar") \
        .getOrCreate()

def read_query_from_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        query = file.read().strip()  # Remove blank spaces
        query = query.rstrip(';') # Remove ";" at the end of file, if exists
    return query

def read_from_postgres(spark: SparkSession, url: str, query: str, user: str, password: str) -> DataFrame:
    return spark.read \
        .format("jdbc") \
        .option("url", url) \
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

# Usage example
if __name__ == "__main__":
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Creating spark session")
    spark = create_spark_session()
    time.sleep(0.5)
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Loading configurations for database connection")
    config = load_config('config.json')
    time.sleep(0.5)
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Loading queries")
    query = read_query_from_file('first_query.sql')
    second_query = read_query_from_file('second_query.sql')
    time.sleep(0.5)
    
    dataframes = []
    database_name = 1
    
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Executing queries on database")
    time.sleep(0.5)
    for db in config['databases']:
        df = read_from_postgres(
            spark, 
            url=db['url'],
            query=query,
            user=db['user'],
            password=db['password']
        )
        print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Executing query on database {database_name}")
        time.sleep(0.5)
        dataframes.append(df)
        print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f" - Query on database {database_name} executed")
        time.sleep(0.5)
        database_name += 1
    
    # Union of dataframes
    combined_df = dataframes[0]
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Combining results from all databases in a dataframe")
    time.sleep(0.5)
    for df in dataframes[1:]:
        combined_df = combined_df.union(df)
    
    # Execution of the second query on combined dataframe
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Executing second query on combined dataframe")
    time.sleep(0.5)
    result_df = execute_query_on_combined_df(spark, combined_df, second_query)
    
    # Save results to a CSV file
    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Saving results to file")
    time.sleep(0.5)
    save_df_to_csv(result_df, "results.csv")

    print( datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " - Results saved to results.csv 🚀")
