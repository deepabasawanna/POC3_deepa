from pyspark.sql import SparkSession
from configparser import ConfigParser
from pyspark.sql.functions import current_date
#from pyspark.sql.functions import col
def main():

    spark = SparkSession.builder.appName("reportgenerator").config("spark.jars", "C:\CGPOC3\postgresql-42.5.2.jar").master("local").getOrCreate()
    config = ConfigParser()

    config_path = "C:/Users/DB4/PycharmProjects/POC3_PROJECT/cgpoc3.properties"
    with open(config_path, "r") as config_file:
        content = config_file.read()

        config.read_string(content)
    #if "parquet_files" in config:
        #parquet_section = config['parquet_files']
        #parquet_file_paths = [parquet_section[key] for key in parquet_section if key.startswith('oppath')]
    #for path in parquet_file_paths:
        #df = spark_session.read.parquet(path) #try to change the loop.
        # Perform operations on the DataFrame as needed
        #df.show() #working correctly till df.show
    # Get the path to the Parquet file from the property file
    parquet_file_path1=config['parquet_files']['oppath1']
    parquet_file_path2=config['parquet_files']['oppath2']
    parquet_file_path3=config['parquet_files']['oppath3']
    parquet_file_path4=config['parquet_files']['oppath4']
    parquet_file_path5=config['parquet_files']['oppath5']
    parquet_file_path6=config['parquet_files']['oppath6']



    # Read the Parquet file into a DataFrame
    df1=spark.read.parquet(parquet_file_path1)
    df2=spark.read.parquet(parquet_file_path2)
    df3=spark.read.parquet(parquet_file_path3)
    df4=spark.read.parquet(parquet_file_path4)
    df5=spark.read.parquet(parquet_file_path5)
    df6=spark.read.parquet(parquet_file_path6)

    # Show the DataFrame
    df1.show()
    df2.show()
    df3.show()
    df4.show()
    df5.show()
    df6.show()
    #print(type(df1))

    #creating temporary view tables
    df1.createOrReplaceTempView("temptable1_cus")
    df2.createOrReplaceTempView("temptable2_items")
    df3.createOrReplaceTempView("temptable3_order_detail")
    df4.createOrReplaceTempView("temptable4_orders")
    df5.createOrReplaceTempView("temptable5_sales")
    df6.createOrReplaceTempView("temptable6_ship_to")

    #creating methods for all reports

    def report1(spark):
        query1 = "SELECT c.cust_id, c.cust_name, COUNT(o.order_id) AS orders_per_week FROM temptable1_cus c JOIN temptable4_orders o ON c.cust_id = o.cust_id GROUP BY c.cust_id, c.cust_name, DATE_TRUNC('week', o.order_date) ORDER BY orders_per_week"
        result1 = spark.sql(query1)
        return result1

    def report2(spark):
        query2 = "SELECT c.cust_id, c.cust_name, SUM(od.item_quantity * od.detail_unit_price) AS total_order_amount FROM temptable1_cus c JOIN temptable4_orders o ON c.cust_id = o.cust_id JOIN temptable3_order_detail od ON o.order_id = od.order_id GROUP BY c.cust_id, c.cust_name, DATE_TRUNC('week', o.order_date) ORDER BY total_order_amount"
        result2=spark.sql(query2)
        return result2

    def report3(spark):
        query3 = "SELECT i.category, COUNT(od.order_id) AS count FROM temptable3_order_detail od JOIN temptable2_items i ON od.item_id = i.item_id GROUP BY i.category ORDER BY count DESC"
        result3 = spark.sql(query3)
        return result3

    def report4(spark):
        query4 = "SELECT od.item_id, i.item_description, SUM(od.item_quantity * od.detail_unit_price) AS total_amount FROM temptable3_order_detail od JOIN temptable2_items i ON od.item_id = i.item_id GROUP BY od.item_id, i.item_description ORDER BY total_amount DESC"
        result4 = spark.sql(query4)
        return result4

    def report5(spark):
        query5 = " SELECT i.category, SUM(od.item_quantity * od.detail_unit_price) AS total_amount FROM temptable3_order_detail od JOIN temptable2_items i ON od.item_id = i.item_id GROUP BY i.category ORDER BY total_amount DESC"
        result5 = spark.sql(query5)
        return result5

    def report6(spark):
        query6 = "SELECT o.order_salesman AS salesman_id, SUM(od.item_quantity * od.detail_unit_price) AS total_sales_amount, SUM(od.item_quantity * od.detail_unit_price) * 0.10 AS incentive_received FROM temptable4_orders o JOIN temptable3_order_detail od ON o.order_id = od.order_id GROUP BY o.order_salesman"
        result6 = spark.sql(query6)
        return result6

    def report7(spark):
        query7 = "SELECT item_id, item_description FROM temptable2_items WHERE item_id NOT IN ( SELECT DISTINCT item_id FROM temptable3_order_detail WHERE DATE_TRUNC('month', created_date) = DATE_TRUNC('month', NOW() - interval '1 month') )"
        result7 = spark.sql(query7)
        return result7

    def report8(spark):
        query8 = "SELECT DISTINCT c.cust_id, c.cust_name FROM temptable1_cus c JOIN temptable4_orders o ON c.cust_id = o.cust_id JOIN temptable6_ship_to s ON o.cust_id = s.cust_id WHERE DATE_TRUNC('month', s.shipping_date) = DATE_TRUNC('month', NOW() - interval '1 month')"
        result8 = spark.sql(query8)
        return result8
    result1 = report1(spark)
    result2 = report2(spark)
    result3 = report3(spark)
    result4 = report4(spark)
    result5 = report5(spark)
    result6 = report6(spark)
    result7 = report7(spark)
    result8 = report8(spark)

    result1_date = result1.withColumn("current_date", current_date())
    result2_date = result2.withColumn("current_date", current_date())
    result3_date = result3.withColumn("current_date", current_date())
    result4_date = result4.withColumn("current_date", current_date())
    result5_date = result5.withColumn("current_date", current_date())
    result6_date = result6.withColumn("current_date", current_date())
    result7_date = result7.withColumn("current_date", current_date())
    result8_date = result8.withColumn("current_date", current_date())
    #result1_date.show()
    #type(result1_date)

    properties = {

        "driver": config.get("Db_Connection", "driver"),

        "user": config.get("Db_Connection", "user"),

        "url": config.get("Db_Connection", "url"),

        "password": config.get("Db_Connection", "password")

    }

    result1_date.write.jdbc(url=properties["url"], table="result1_date", mode="overwrite", properties=properties)
    result2_date.write.jdbc(url=properties["url"], table="result2_date", mode="overwrite", properties=properties)
    result3_date.write.jdbc(url=properties["url"], table="result3_date", mode="overwrite", properties=properties)
    result4_date.write.jdbc(url=properties["url"], table="result4_date", mode="overwrite", properties=properties)
    result5_date.write.jdbc(url=properties["url"], table="result5_date", mode="overwrite", properties=properties)
    result6_date.write.jdbc(url=properties["url"], table="result6_date", mode="overwrite", properties=properties)
    result7_date.write.jdbc(url=properties["url"], table="result7_date", mode="overwrite", properties=properties)
    result8_date.write.jdbc(url=properties["url"], table="result8_date", mode="overwrite", properties=properties)

    result1_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result1_date")
    result2_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result2_date")
    result3_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result3_date")
    result4_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result4_date")
    result5_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result5_date")
    result6_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result6_date")
    result7_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result7_date")
    result8_date.write.mode("overwrite").partitionBy("current_date").parquet("C:/Users/DB4/PycharmProjects/POC3_PROJECT/output2/result8_date")


if __name__ == '__main__':
    main()


