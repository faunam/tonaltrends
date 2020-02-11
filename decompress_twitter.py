import boto3
import tarfile
import os

s3_client = boto3.client('s3')


def generate_filenames():
    files_to_get = []
    for year in range(2015, 2017):
        for month in range(1, 13):
            if year == 2015 and month in list(range(1, 6)):
                continue
            month_padding = "0" if month < 10 else ""
            date_str = str(year) + "-" + month_padding + str(month)
            filename = 'archiveteam-twitter-stream-{}.tar'.format(
                date_str)
            files_to_get.append(filename)
    return files_to_get

#!! check if i can process bz2s before uploading everything. not super critical though,
# becuase if im able to upload thats the main thing.


def decompress(filename, dest_bucket):
    # re: "r|", see https://docs.python.org/3/library/tarfile.html
    # supposed to be "r|" for streaming but i was getting a backpass error i didnt feel like dealing with
    s3_client.download_file("twitter-data-insight", filename,
                            "/home/ubuntu/temp/"+filename)
    with tarfile.open("/home/ubuntu/temp/"+filename, "r") as tf:
        for entry in tf:
            filename = entry.name
            if filename[-4:] == ".bz2":
                # might already be extracted, may not need to do extractfile
                fileobj = tf.extractfile(entry)  # buffer??
                # fileobj is now an open file object. Use `.read()` to get the data.
                # alternatively, loop over `fileobj` to read it line by line.
                s3_client.upload_fileobj(
                    fileobj,
                    Bucket=dest_bucket,
                    Key=filename
                )
                print(fileobj)
                # upload_bzs(fileobj)
    # delete when done TODO


file_list = generate_filenames()
print(file_list)
for elt in file_list:
    decompress(elt,
               "s3://twitter-decompressed-insight")
    os.remove("/home/ubuntu/temp/" + elt)

# zip_obj = s3_resource.Object(bucket_name="bucket_name_here", key=zip_key)
# buffer = BytesIO(zip_obj.get()["Body"].read())

# z = zipfile.ZipFile(buffer)
# for filename in z.namelist():
#     file_info = z.getinfo(filename)
#     s3_resource.meta.client.upload_fileobj(
#         z.open(filename),
#         Bucket=bucket,
#         Key=f'{filename}'
#     )
