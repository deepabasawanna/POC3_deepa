from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import col
def main():
    # For creating Spark Session
    spark = SparkSession.builder.config("spark.jars", "C:\CGPOC3\postgresql-42.5.2.jar").appName("jdbc").master("local").getOrCreate()

    # Accessing the proprties file which i have created under config folder i.e., Config.properties file

    config = ConfigParser()

    config_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/cgpoc3.properties"

    with open(config_path, "r") as config_file:
        content = config_file.read()

        config.read_string(content)

    properties = {

        "driver": config.get("Db_Connection", "driver"),

        "user": config.get("Db_Connection", "user"),

        "url": config.get("Db_Connection", "url"),

        "password": config.get("Db_Connection", "password")

    }



    op_path = config.get("output_path", "outputpath")
    table_query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'  
    """

    table_df = spark.read \
        .format("jdbc") \
        .option("url", properties["url"]) \
        .option("dbtable", f"({table_query}) AS tables") \
        .option("user", properties["user"]) \
        .option("password", properties["password"]) \
        .option("driver", properties["driver"]) \
        .load()
    #pandas_df = table_df.toPandas()

    # Convert pandas DataFrame to a dictionary
    #result_dict= pandas_df.to_dict(orient="records")
    #values_list = list(result_dict.values())
    #table_names = table_df.select("table_name").rdd.flatMap(lambda x: x).collect()

    #table_list = list(table_names)
    values_list = [row[0] for row in table_df.select(col("table_name")).collect()]

    for i in values_list:
        data = spark.read.jdbc(url=properties["url"], table=i, properties=properties)

        data.show()

        data.write.parquet(op_path.format(str(i)))


if __name__ == '__main__':
    main()
