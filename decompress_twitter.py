# refactor everything.. i think you can read tars from s3??
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
    print("downloaded!")


def check_uploaded(filename, destination_bucket):
    bucket = s3.Bucket(destination_bucket)
    key = filename
    objs = list(bucket.objects.filter(Prefix=key))
    print("Checking upload...")
    if len(objs) > 0 and objs[0].key == key:
        print("All good!")
    else:
        # custom error?
        raise UploadError(objs, filename + " was not uploaded!")

# if has tar inside, wont make a folder. maybe like for each folder: "while files in os.listdir() are zips or tars"
# unzip or untar, delete original.
# check if made a folder, if so, enter, if not, check for other files that need to be decompressed


def unpack_and_upload(filename, ids, temp_folder, destination_bucket):
    filepath = temp_folder + filename
    if os.path.isdir(filepath):
        for file in os.listdir(filepath):
            print(file)
            unpack_and_upload(
                file, ids + [filename], filepath + "/", destination_bucket)

    # unpack
    elif filename[-4:] == ".zip":
        # call_command_line("unzip {} && rm {}".format(filename, filename))
        with zipfile.ZipFile(filepath, "r") as z:
            z.extractall(temp_folder)
        print("unzipped!")
        os.remove(filepath)
        if os.path.isdir(filepath[:-4]):
            unpack_and_upload(
                filename[:-4], ids, temp_folder, destination_bucket)
        else:
            try:
                unpack_and_upload(
                    filename[:-4] + ".tar", ids, temp_folder, destination_bucket)
            except:
                print("not tar!!")
                print(os.listdir(temp_folder))

    elif filename[-4:] == ".tar":
        with tarfile.TarFile(filepath, "r") as t:
            t.extractall(temp_folder)
        print("untarred!")
        os.remove(filepath)
        unpack_and_upload(
            filename[:-4], ids, temp_folder, destination_bucket)

    # upload
    elif filename[-4:] in [".bz2", "json"]:
        full_name = "-".join(ids + [filename])
        s3_client.upload_file(
            filepath, destination_bucket, full_name)
        # CHECK IF UPLOADED
        check_uploaded(full_name, destination_bucket)
        print("uploaded" + filename[-4:])
    else:
        return


def download_unpack_upload(source_bucket, temp_folder, destination_bucket):
    # if dir, call unpack recursively on contents
    # if zip, unzip, delete zip, call unpack on file
    # if tar, untar, delete tar, and call unpack on file
    # dont need to unzip bz2, spark may not be able to split them so maybe some performance considerations here but yeah
    bucket = s3.Bucket(source_bucket)
    for obj in bucket.objects.all():
        filename = obj.key
        # download
        download(bucket.name, filename, temp_folder)
        # unpack & upload; assign appropriate informative filenames
        unpack_and_upload(filename,
                          [], temp_folder, destination_bucket)
        # TODO ERROR HANDLING??? if not uploaded??
        # delete
        try:
            # because the compression extension has been removed
            shutil.rmtree(temp_folder + filename[:-4])
        except OSError as e:
            print("Error: %s : %s" % (filename[:-4], e.strerror))
        # ***make sure you've put like error breaks in so it doesnt delete before upload


# download_unpack_upload("fauna-ex", "/home/ubuntu/temp/",
#                       "twitter-decompressed-insight")

def one_file(source_bucket, filename, temp_folder, destination_bucket):
    download(source_bucket, filename, temp_folder)
    # unpack & upload; assign appropriate informative filenames
    unpack_and_upload(filename,
                      [], temp_folder, destination_bucket)
    # TODO ERROR HANDLING??? if not uploaded??
    # delete
    try:
            # because the compression extension has been removed
        shutil.rmtree(temp_folder + filename[:-4])
    except OSError as e:
        print("Error: %s : %s" % (filename[:-4], e.strerror))


one_file("twitter-data-insight", "archiveteam-twitter-stream-2015-03.zip",
         "/home/ubuntu/temp/", "twitter-decompressed-insight")
