from pyspark.sql import SparkSession
#from configparser import ConfigParser
from configparser import ConfigParser
from pyspark.sql.functions import current_date

def create_spark_session():
    spark = SparkSession.builder.config("spark.jars", "C:\CGPOC3\postgresql-42.5.2.jar").appName("incrementaldataload").master("local").getOrCreate()
    return spark


def read_parquet_file(spark, file_path):
    return spark.read.parquet(file_path)

def cust_updated():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/customers"
    df = spark.read.parquet(parquet_file_path)
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="customers", properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/cus")

def Items_updated():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/items"
    df = spark.read.parquet(parquet_file_path)
   # df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="items" , properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/it")

def order_details():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/order_details"
    df = spark.read.parquet(parquet_file_path)
  #  df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="order_details", properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/odd")

def orders():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/orders"
    df = spark.read.parquet(parquet_file_path)
    #df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="orders", properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/od")

def salesperson():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/salesperson"
    df = spark.read.parquet(parquet_file_path)
   # df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="salesperson", properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/sale_ppl")

def ship_to():

    parquet_file_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/output1/ship_to"
    df = spark.read.parquet(parquet_file_path)
    #df.show()
    latest_date = df.agg({"created_date": "max"}).collect()[0][0]
    db_df = spark.read.jdbc(url=properties["url"], table="ship_to", properties=properties)
    db_df1 = db_df.filter(db_df.created_date > latest_date)
    db_df1.show()
    db_df1.write.mode("overwrite").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output3/ship")

if __name__ == "__main__":

    spark = create_spark_session()
    config = ConfigParser()
    config_path = 'C:/Users/DB4/PycharmProjects/POC3_PROJECT/cgpoc3.properties'
    with open(config_path, "r") as config_file:
        content = config_file.read()
        config.read_string(content)
    properties = {

        "driver": config.get("Db_Connection", "driver"),

        "user": config.get("Db_Connection", "user"),

        "url": config.get("Db_Connection", "url"),

        "password": config.get("Db_Connection", "password")

    }
    cust_updated()
    Items_updated()
    order_details()
    orders()
    salesperson()
    ship_to()
