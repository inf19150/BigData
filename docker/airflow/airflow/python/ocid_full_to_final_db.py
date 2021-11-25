import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext
import argparse


def get_args():
    """
    Parses Command Line Args
    """
    parser = argparse.ArgumentParser(
        description='Spark Job filters db and stores it to final_db onto HDFS as well to mysql')
    parser.add_argument(
        '--year', help='Partion Year To Process, e.g. 2019', required=True, type=str)
    parser.add_argument(
        '--month', help='Partion Month To Process, e.g. 10', required=True, type=str)
    parser.add_argument(
        '--day', help='Partion Day To Process, e.g. 31', required=True, type=str)
    parser.add_argument(
        '--hdfs_source_dir', help='HDFS source directory, e.g. /user/hadoop/ocid/raw', required=True, type=str)
    parser.add_argument(
        '--hdfs_target_dir', help='HDFS target directory, e.g. /user/hadoop/ocid/final', required=True, type=str)

    return parser.parse_args()


if __name__ == '__main__':
    """
    Main Function
    """

    # Parse Command Line Args
    args = get_args()

    # Initialize Spark Context
    sc = pyspark.SparkContext()
    spark = SparkSession(sc)

    # Read full-table from csv-file
    schema = StructType(
        [
            StructField("radio", StringType(), True),
            StructField("mcc", IntegerType(), True),
            StructField("net", IntegerType(), True),
            StructField("area", IntegerType(), True),
            StructField("cell", IntegerType(), True),
            StructField("unit", IntegerType(), True),
            StructField("lon", DoubleType(), True),
            StructField("lat", DoubleType(), True),
            StructField("range", IntegerType(), True),
            StructField("samples", IntegerType(), True),
            StructField("changeable", IntegerType(), True),
            StructField("created", IntegerType(), True),
            StructField("updated", IntegerType(), True),
            StructField("averageSignal", IntegerType(), True)
        ])

    df_full = spark.read.format("csv").options(
        header="true",
        delimiter=",",
        nullValue="null",
        inferSchema="false"
    ).schema(schema).load(args.hdfs_source_dir + "ocid_full_" + args.year + "-" + args.month + "-" + args.day + ".csv")

    # filter relevant cols
    df_full = df_full.select("radio", "lat", "lon", "range")

    # germany only
    df_full = df_full.filter(col("lat") >= 46.959149).filter(col(
        "lon") >= 5.704355).filter(col("lat") <= 55.019144).filter(col("lon") <= 15.186580)

    df_full.printSchema()
    df_full.count()
    df_full.show()

    # repartition by radio-type and append (save) to final_table (hdfs parquet)
    df_full.repartition('radio').write.format("parquet").mode("append").option(
        "path", args.hdfs_target_dir).partitionBy("radio").saveAsTable("default.lat_lon_range")

    df_full.write.format('jdbc').options(
        url='jdbc:mysql://mysql:3306/final_db',
        driver='com.mysql.jdbc.Driver',
        dbtable='Fulldatabase',
        user="root",
        password="bigdata2021").mode('append').save()
