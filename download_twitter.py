# https://archive.org/details/twitterstream start in 2015? i think the gkg only starts in 2015
def generate_urls():
    urls_to_get = []
    for year in range(2015, 2020):
        for month in range(1, 13):
            month_padding = "0" if month < 10 else ""
            date_str = str(year) + "-" + month_padding + str(month)
            first = f'https://archive.org/compress/archiveteam-twitter-stream-{date_str}/formats=TAR&file=/archiveteam-twitter-stream-{date_str}.zip'
            second = f'https://archive.org/download/archiveteam-twitter-stream-{date_str}/archiveteam-twitter-stream-{date_str}.tar'
            urls_to_get.append((first, second))
    # with open("newfile.txt", "w") as outfile:
    #     [outfile.write(elt[0] + ", " + elt[1] + "\n") for elt in urls_to_get]
    return urls_to_get


def unpack(filename):
    # check filename, if zip, unzip (to same name just without zip) and delete zip, then enter file and call unpack recursively on file contents
    # if tar, untar, delete tar, and call recursively on file contents
    # if bz2, unbz and delete bz
    pass


def upload(filepath):
    # filepath is a directory. might have JSONS, might have more directories with JSONS
    # iterate. if dir, call upload on it
    # if file, upload to s3. should i put them in files on s3? whats the point...
    pass


def down_up_file(pair, target_folder):
    try:
        try:
            "wget " + pair[0] + " " + target_folder
            filename = pair[0]  # regex the part after the last /  in the url
        except:
            "wget " + pair[1] + " " + target_folder
            filename = pair[1]  # regex the part after the last /  in the url

        unpack(filename)
        upload(filename[:-4])
        # do i need to wait for things to unzip before i ask to untar? same with waiting for unpack before uploading?
        # delete after uploading -> i think there are built in CLI commands for this
    except:  # there must be a better way..
        print(pair[1])


url_list = generate_urls()
for elt in url_list:
    down_up_file(elt, "/home/ubuntu/twitter_data_temp/")
