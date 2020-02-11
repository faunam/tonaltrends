import subprocess


def generate_script():
    with open("decompress.sh", "w") as outfile:
        outfile.write("#!/bin/bash\n")
        for year in range(2015, 2017):
            for month in range(1, 13):
                if year == 2015 and month in list(range(1, 6)):
                    continue
                month_padding = "0" if month < 10 else ""
                date_str = str(year) + "-" + month_padding + str(month)
                script_line = """sudo tar -xvf /home/ubuntu/s3_link_temp/archiveteam-twitter-stream-{}.tar -C /home/ubuntu/s3_link_temp/decompressed""".format(
                    date_str)
                # --xform='s#^.+/##x' - xform removes directory structure
                outfile.write(script_line + "\n")


subprocess.check_call("touch decompress.sh".split(" "))
generate_script()
subprocess.check_call("chmod 755 decompress.sh".split(" "))
subprocess.check_call("./decompress.sh")


def flatten():
    # need to put directory names in filenames (or like )
    pass
