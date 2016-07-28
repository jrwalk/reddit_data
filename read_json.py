"""Reads monthly JSON dumps, writes out to storage JSON files.
"""

import json
import datetime as dt
import os
import bz2
import pymysql as pms
from collections import OrderedDict
import warnings
warnings.simplefilter('ignore')


def process_zip(filepath,subreddits):
    """all-in-one processor for zipped files.

    ARGS:
        filepath: string.
            path to zipped file to be processed.
        subreddits: list.
            list of strings containing names of target subreddits to be saved.
    """
    entries_read = 0
    entries_saved = 0
    writefilepath = (filepath.split('.')[0]+'_scraped_{}'
                     .format(dt.datetime.now()))

    try:
        start = dt.datetime.now()
        with bz2.BZ2File(filepath,'r') as file:
            with open(writefilepath,'w') as writefile:
                for line in file:
                    data = json.loads(line.decode())
                    entries_read += 1

                    sub = data.get('subreddit',None)
                    if sub in subreddits:
                        entries_saved += 1
                        json.dump(data,writefile)
                        writefile.write('\n')
        end = dt.datetime.now()

        with open('read_json.log','a') as logfile:
            logfile.write("reading file {}\n".format(filepath))
            logfile.write("read started at {}\n".format(start))
            logfile.write("read {} JSON objects, saved {} "
                      "JSON objects ({:.2f}% saved)\n"
                      .format(entries_read,entries_saved,
                              entries_saved/entries_read*100.))
            logfile.write("read finished at {}\n".format(end))
            logfile.write("read duration: {}\n".format(end-start))
            logfile.write('\n')

    except:
        with open('read_json.log','a') as logfile:
            logfile.write('read file {} failed at {}\n'
                          .format(filepath,dt.datetime.now()))
            logfile.write('\n')
        with open('failed_downloads.log','a') as failedfile:
            failedfile.write(filepath)
            failedfile.write('\n')

    os.remove(filepath)


def read_to_sql(file, conn):
    """wrapper to read scraped JSON files into appropriate SQL table.

    ARGS:
        file: string.
            path to scraped JSON file.
        conn: pymysql.Connect object.
            connection to server.
    """
    cur = conn.cursor()

    if 'RC' in file:
        table = 'Comments'
    else:
        table = 'Submissions'

    with open(file,'r') as readfile:
        count = 0
        for line in readfile:
            count += 1
            data = json.loads(line)
            input_data = OrderedDict()

            cur.execute("SHOW COLUMNS FROM {}".format(table))
            for row in cur:
                field = row[0]
                input_data[field] = data.get(field,None)

            keys = ', '.join(input_data.keys())
            values = ', '.join(['%s'] * len(input_data))
            query = ("INSERT IGNORE INTO {} ({}) VALUES ({});"
                     .format(table,keys,values))
            cur.execute(query,tuple(input_data.values()))

    print("inserted {} rows".format(count))
    conn.commit()


def read_all_to_sql(dl_dir):
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')

    files = os.listdir(dl_dir)
    files = [dl_dir+'/'+f for f in files]
    count = len(files)
    for i,file in enumerate(files):
        print("{}/{} writing {}".format(i+1,count,file))
        read_to_sql(file,conn)