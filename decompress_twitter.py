# refactor everything.. i think you can read tars from s3??
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import boto3
import zipfile
import tarfile
from io import BytesIO
import os
import shutil

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')


class UploadError(Exception):
    """Exception raised for errors in upload to S3.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message


def download(bucket, filename, destination):
    # destination must end in /
    s3_client.download_file(bucket, filename, destination + filename)


def check_uploaded(filename, destination_bucket):
    bucket = s3.Bucket(destination_bucket)
    key = filename
    objs = list(bucket.objects.filter(Prefix=key))
    if len(objs) != 0 or objs[0].key != key:
        # custom error?
        raise UploadError(objs, filename + " was not uploaded!")


def unpack_and_upload(filename, id, destination_bucket):
    if os.path.isdir(filename):
        id = id + "-" + filename
        for file in os.listdir(filename):
            unpack_and_upload(file, id, destination_bucket)

    # unpack
    elif filename[-4:] == ".zip":
        # call_command_line("unzip {} && rm {}".format(filename, filename))
        with zipfile.ZipFile(filename, "r") as z:
            z.extractall("~/temp/")
        # the folder that was inside of the zip
        unpack_and_upload(
            "~/temp/"+filename[:-4], filename[:-4], destination_bucket)
    elif filename[-4:] == ".tar":
        with tarfile.TarFile(filename, "r") as t:
            t.extractall("~/temp/")
        if id == "":
            id = filename[:-4]
        else:
            id = id + "-" + filename[:-4]
        unpack_and_upload("~/temp/"+filename[:-4], id, destination_bucket)
    # upload
    elif filename[-4:] == ".bz2":
        boto3.upload_file(filename, destination_bucket, id + filename)
        # CHECK IF UPLOADED
        check_uploaded(id + filename, destination_bucket)
    else:
        return


def download_unpack_upload(source_bucket, destination_bucket):
    # if dir, call unpack recursively on contents
    # if zip, unzip, delete zip, call unpack on file
    # if tar, untar, delete tar, and call unpack on file
    # dont need to unzip bz2, spark may not be able to split them so maybe some performance considerations here but yeah
    bucket = s3.Bucket(source_bucket)
    for filename in bucket.objects:
        # download
        download(bucket, filename, "~/temp/")
        # unpack & upload; assign appropriate informative filenames
        unpack_and_upload("~/temp /" + filename, "", destination_bucket)
        # TODO ERROR HANDLING??? if not uploaded??
        # delete
        try:
            # because the compression extension has been removed
            shutil.rmtree(filename[:-4])
        except OSError as e:
            print("Error: %s : %s" % (filename[:-4], e.strerror))
        # ***make sure you've put like error breaks in so it doesnt delete before upload


download_unpack_upload("fauna-ex", "twitter-decompressed-insight")
