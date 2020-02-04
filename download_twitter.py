import subprocess
import os

# https://archive.org/details/twitterstream start in 2015? i think the gkg only starts in 2015


def generate_urls():
    urls_to_get = []
    for year in range(2015, 2020):
        for month in range(1, 13):
            if year == 2015 and month in list(range(3, 7)):
                continue
            month_padding = "0" if month < 10 else ""
            date_str = str(year) + "-" + month_padding + str(month)
            first = 'https://archive.org/compress/archiveteam-twitter-stream-{}/formats=TAR&file=/archiveteam-twitter-stream-{}.zip'.format(
                date_str, date_str)
            second = 'https://archive.org/download/archiveteam-twitter-stream-{}/archiveteam-twitter-stream-{}.tar'.format(
                date_str, date_str)
            urls_to_get.append((first, second))
    # with open("newfile.txt", "w") as outfile:
    #     [outfile.write(elt[0] + ", " + elt[1] + "\n") for elt in urls_to_get] #to check
    return urls_to_get


def call_command_line(string, **kwargs):
    """Executes string as a command line prompt. stdout and stderr are keyword args."""
    return subprocess.check_call(string.split(" "), **kwargs)


def command_suite(url, temp_folder, s3_bucket):
    filename = url.split("/")[-1]
    call_command_line(
        "wget -P " + temp_folder + " " + url)  # stdout="twitter_out.txt"
    call_command_line("aws s3 cp " + temp_folder +
                      filename + " " + s3_bucket)
    call_command_line("rm " + temp_folder + filename)


def down_up_file(pair, temp_folder, s3_bucket):
    # target folder must end in "/"
    try:
        try:  # zip
            command_suite(pair[0], temp_folder, s3_bucket)
        except:  # tar
            command_suite(pair[1], temp_folder, s3_bucket)

        # unpack(filename, filename[:-4], s3_sync_folder) #maybe do this after i download from s3
        # upload(temp_folder + filename)  # maybe call upload on s3 synced folder
        # do i need to wait for things to unzip before i ask to untar? same with waiting for unpack before uploading?
        # rm(temp_folder + filename)  # delete container files, and delete files in s3 synced bucket. wait if i dont have the full contents will the sync work?
        # delete after uploading -> i think there are built in CLI commands for this
    except:  # there must be a better way..
        print("none " + pair[1])


url_list = generate_urls()
for elt in url_list:
    down_up_file(elt, "/home/ubuntu/twitter_data_temp/",
                 "s3://twitter-data-insight/")
