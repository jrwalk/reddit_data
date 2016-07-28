"""cleanup scripts for handling failed downloads, for use in rerunning script.
"""

import os


def get_fails():
    """extracts list of failed downloads from logfile.
    """
    failed_downloads = []
    with open('failed_downloads.log','r') as logfile:
        for line in logfile:
            failed_downloads.append(line)

    print("{} bad files".format(len(failed_downloads)))
    return set(failed_downloads)


def clean_dir(dl_dir):
    """removes bad download files from `dir`.

    ARGS:
        dl_dir: string.
            path to target download directory.
    """
    failed_downloads = get_fails()
    failed_targets = set([f[4:14] for f in failed_downloads])

    dl_files = os.listdir(dl_dir)
    for file in dl_files:
        if file[:10] in failed_targets:
            rem = dl_dir+'/'+file
            os.remove(rem)
            print("removed {}".format(rem))

    os.remove('failed_downloads.log')
    open('failed_downloads.log','w').close()
