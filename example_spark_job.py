from pyspark.sql import SparkSession
#import java.sql
# from pyspark.sql import SaveMode
# from pyspark.sql import JDBCOptions

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GDELTToneTrends")\
        .getOrCreate()

    df = spark.read.option("sep", "\t").csv(
        "s3a://gdelt-sample/20200116003000.gkg.csv")

    # remove irrel cols
    col_nums_to_keep = [0, 1, 2, 4, 7, 9, 11, 13, 15, 26]
    cols_to_drop = []
    for col_num in range(len(df.columns)):
        if col_num not in col_nums_to_keep:
            cols_to_drop.append("_c" + str(col_num))

    df = df.drop(*cols_to_drop)

    # write to db
    db_url = "jdbc:postgresql://tone-db.ccg3nx1k7it5.us-west-2.rds.amazonaws.com:5432/postgres"
    db_properties = {"user": "faunam", "password": "",
                     "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=db_url, table="tonedb",
                  mode="overwrite", properties=db_properties)
# df.write.mode(SaveMode.Append).option(JDBCOptions.JDBC_DRIVER_CLASS,
#                                      "org.postgresql.Driver").jdbc(db_url, dbTable, db_properties)

    spark.stop()

# pyspark --driver-class-path /usr/local/spark/jars/postgresql-42.2.9.jar --jars /usr/local/spark/jars/postgresql-42.2.9.jar
# spark-submit --driver-class-path /usr/local/spark/jars/postgresql-42.2.9.jar --jars /usr/local/spark/jars/postgresql-42.2.9.jar example.py
