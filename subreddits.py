"""Processes subreddit index into SQL table.
"""


import json
import pymysql as pms

def process(subreddit_file):
    """processes subreddit text file into SQL table.

    ARGS:
        subreddit_file: string.
            filepath to subreddit file, a text file of JSONs.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8',
                       init_command='SET NAMES UTF8; SET CHARACTER SET utf8;')
    cur = conn.cursor()

    with open(subreddit_file,'r') as readfile:
        for line in readfile:
            data = json.loads(line)

            url = data['url']
            lang = data['lang']
            public_description = data['public_description']
            subreddit_type = data['subreddit_type']
            header_title = data['header_title']
            name = data['name']
            subscribers = data['subscribers']
            quarantine = data['quarantine']
            title = data['title']
            sub_id = data['id']
            created_utc = data['created_utc']
            over18 = data['over18']
            values = (url,title,sub_id,name,created_utc,lang,subreddit_type,
                      over18,subscribers,quarantine)

            query = ("INSERT INTO Subreddits "
                     "(url,"
                     "title,"
                     "`id`,"
                     "name,"
                     "created_utc,"
                     "lang,"
                     "subreddit_type,"
                     "over18,"
                     "subscribers,"
                     "quarantine) "
                     "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
            try:
                cur.execute(query,values)
            except:
                print('skipping {}'.format(url))

    conn.commit()
    conn.close()