import subprocess
import re
from pyspark.sql import SparkSession

import process_gdelt
import db_info


def call_command_line(string, **kwargs):
    """Executes string as a command line prompt. stdout and stderr are keyword args."""
    return subprocess.check_call(string.split(" "), **kwargs)


def get_csv_url(index_url, temp_dir):
    call_command_line("wget -O " + temp_dir + "csv_list.txt " + index_url)
    with open(temp_dir + "csv_list.txt", "r") as csv_list:
        pattern = re.compile("\.gkg\.csv\.zip")
        for line in csv_list:
            if re.search(pattern, line):  # if result exists
                csv_url = line.strip().split(" ")[-1]
    try:
        return csv_url
    except:
        raise EOFError("no gkg file found")


def download_unzip_new_csv(index_url, temp_dir):
    # s3_mount_dir must end in /
    # returns filename as side effect
    csv_url = get_csv_url(index_url, temp_dir)
    csv_zip_filename = csv_url.split("/")[-1]
    # might have to precede all these with sudo
    call_command_line("wget -O " + temp_dir + csv_zip_filename + " " + csv_url)
    call_command_line("unzip " + temp_dir +
                      csv_zip_filename + " -d " + temp_dir)
    return temp_dir + csv_zip_filename[:-4]


def process_and_upload_csv(filepath, psql_table):
    spark = SparkSession\
        .builder\
        .appName("NewGDELT")\
        .getOrCreate()

    gdelt_df = process_gdelt.format(
        spark, filepath)
    gdelt_df.write.jdbc(url=db_info.url, table=psql_table,
                        mode="append", properties=db_info.properties)
    spark.stop()


def down_up(psql_table):
    # TODO should really be passing in psql database info rather than just the table name; makes it more modular
    index_url = 'http://data.gdeltproject.org/gdeltv2/lastupdate.txt'
    temp_dir = "/home/ubuntu/very_temp/"

    # call_command_line("mkdir " + temp_dir)
    filepath = download_unzip_new_csv(index_url, temp_dir)
    # might need to check if csv is in s3 bucket before proceeding to next step. airflow?
    # problem with processing - it cant find people column?
    process_and_upload_csv(filepath, psql_table)
    #call_command_line("rm -r " + temp_dir)
