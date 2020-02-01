import subprocess
import os

# https://archive.org/details/twitterstream start in 2015? i think the gkg only starts in 2015


def generate_urls():
    urls_to_get = []
    for year in range(2015, 2020):
        for month in range(1, 13):
            month_padding = "0" if month < 10 else ""
            date_str = str(year) + "-" + month_padding + str(month)
            first = 'https://archive.org/compress/archiveteam-twitter-stream-{}/formats=TAR&file=/archiveteam-twitter-stream-{}.zip'.format(
                date_str, date_str)
            second = 'https://archive.org/download/archiveteam-twitter-stream-{}/archiveteam-twitter-stream-{}.tar'.format(
                date_str, date_str)
            urls_to_get.append((first, second, date_str))
    # with open("newfile.txt", "w") as outfile:
    #     [outfile.write(elt[0] + ", " + elt[1] + "\n") for elt in urls_to_get] #to check
    return urls_to_get


def call_command_line(string, **kwargs):
    """Executes string as a command line prompt. stdout and stderr are keyword args."""
    return subprocess.check_call(string.split(" "), **kwargs)


def unpack(filename, id, destination):
    # if dir, call unpack recursively on contents
    # if zip, unzip, delete zip, call unpack on file
    # if tar, untar, delete tar, and call unpack on file
    # if bz2, unbz and delete bz -> may or may not need to do this. apparently spark can handle them but theyre not splittable. so idk if it will affect performance or not. for now we can just leave it i think?
    if os.path.isdir(filename):
        for file in os.listdir(filename):
            unpack(file, id + "-" + file[:-4], destination)
    elif filename[-4:] == ".zip":
        call_command_line("unzip {} && rm {}".format(filename, filename))
        unpack(filename, id, destination)
    elif filename[-4:] == ".tar":
        call_command_line("tar xf {} && rm {}".format(filename, filename))
        unpack(filename, id, destination)
    elif filename[-4:] == ".bz2":
        # maybe i shuold unzip to s3 synced folder? in that case name them something unique.
        call_command_line("bzip2 -d {} > {}".format(filename,
                                                    destination + id + "-" + filename[:-4]))
    else:
        return


def upload(filepath):
    if os.path.isdir(filepath):
        for file in os.listdir(filepath):
            upload(file)
    else:
        call_command_line()

        # filepath is a directory. might have JSONS, might have more directories with JSONS
        # iterate. if dir, call upload on it
        # if file, upload to s3. should i put them in files on s3? whats the point...


def down_up_file(pair, temp_folder):
    # target folder must end in "/"
    try:
        try:
            outfile = open("stdout/twitter_" + pair[2] + "_out.txt", "w")
            call_command_line(
                "wget " + pair[0] + " " + temp_folder, stdout=outfile)
            outfile.close
            print("zip")
            # filename = pair[0]  # regex the part after the last /  in the url
        except:
            outfile = open("stdout/twitter_" + "tar_" +
                           pair[2] + "_out.txt", "w")
            call_command_line(
                "wget " + pair[1] + " " + temp_folder, stdout=outfile)
            outfile.close
            print("tar" + pair[2])
            # filename = pair[1]  # regex the part after the last /  in the url

        # unpack(filename, filename[:-4], s3_sync_folder) #maybe do this after i download from s3
        # upload(temp_folder + filename)  # maybe call upload on s3 synced folder
        # do i need to wait for things to unzip before i ask to untar? same with waiting for unpack before uploading?
        # rm(temp_folder + filename)  # delete container files, and delete files in s3 synced bucket. wait if i dont have the full contents will the sync work?
        # delete after uploading -> i think there are built in CLI commands for this
    except:  # there must be a better way..
        print("none" + pair[2])


url_list = generate_urls()
# outfile = open("twitter_out.txt", "w")
# outfile.close
for elt in url_list:
    print(elt)
    down_up_file(elt, "/home/ubuntu/twitter_data_temp/")
