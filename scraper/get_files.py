"""Pulls filenames from files.pushshift.io, parses out into list of filenames 
available for download.
"""

import requests
from bs4 import BeautifulSoup
import datetime as dt
import pymysql as pms
import os

def list_files():
    """pulls listings of comment and submission dumps from files.pushshift.io.

    RETURNS:
        coms: set.
            set of strings detailing relative filepath on 
            files.pushshift.io/comments/ for repositories of comments
        subs: set.
            set of strings detailing relative filepath on 
            files.pushshift.io/submissions/ for repositories of submissions
    """
    subs = requests.get('http://files.pushshift.io/reddit/submissions')
    coms = requests.get('http://files.pushshift.io/reddit/comments')

    subtext = subs.text
    subs.close()
    comtext = coms.text
    coms.close()

    subs = BeautifulSoup(subtext,'html.parser')
    coms = BeautifulSoup(comtext,'html.parser')

    # pull submission links
    arows = subs.find_all('a',href=True)
    links = [a['href'] for a in arows]
    links = set(links)  # strip down to unique values
    subs = set([x for x in links if 'RS' in x])   # remove checksum, donation

    # pull comment links
    arows = coms.find_all('a',href=True)
    links = [a['href'] for a in arows]
    links = set(links)
    coms = set([x for x in links if 'RC' in x])

    return (coms,subs)


def restrict_files(subreddits,files):
    """checks the establishment dates for each subreddit, drops files earlier 
    than the earliest date from the file list to save download time.

    ARGS:
        subreddits: iterable.
            list of strings containing subreddit names.
        files: iterable.
            iterable containing the names of the files to download.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8')
    cur = conn.cursor()

    dates = []
    for sub in subreddits:
        url = '/r/'+sub+'/'
        cur.execute("SELECT created_utc FROM Subreddits WHERE url=%s",(url,))
        created_utc = int(cur.fetchone()[0])
        created_utc = dt.datetime.utcfromtimestamp(created_utc)
        dates.append(created_utc)

    conn.close()

    earliest_date = min(dates).date()
    earliest_date = earliest_date.replace(day=1)

    subset = files.copy()

    for file in list(subset):
        date = file[5:-4]
        month = dt.datetime.strptime(date,'%Y-%m').date()
        if month < earliest_date:
            subset.remove(file)

    return subset


def restrict_files_to_dir(files,dl_dir):
    """checks the scraped/downloaded files already in `dir`, removes these from 
    the freshly-generated download queue.

    ARGS:
        files: iterable.
            iterable containing names of files to download.
        dir: string.
            path to download directory to check.
    """
    downloaded_files = os.listdir(dl_dir)
    downloaded_files = ['./'+file[:10]+'.bz2' for file in downloaded_files]
    downloaded_files = set(downloaded_files)

    files = set(files)
    return files.difference(downloaded_files)


def restrict_files_to_db(files):
    """checks for latest date of scraped posts in DB, restricts files to only 
    non-represented blocks.

    ARGS:
        files: iterable.
            iterable containing names of files to download.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8')
    cur = conn.cursor()
    query = """SELECT DATE(FROM_UNIXTIME(created_utc)) 
            FROM Submissions 
            ORDER BY created_utc DESC 
            LIMIT 1;
            """
    cur.execute(query)
    lastdate = cur.fetchone()[0]

    subset = files.copy()
    for file in list(subset):
        filedate = dt.datetime.strptime(file[5:-4],'%Y-%m').date()
        if filedate <= lastdate:
            subset.remove(file)
    return subset


def download(filepathURL):
    """for the given filepath URL, download the target file.

    ARGS:
        filepathURL: string.
            relative link on files.pushshift.io, in the form
            './R[C,S]_[YYYY]-[MM].bz2'.  Raw output in sets produced by 
            `list_files`.
    """
    filepathURL = filepathURL[2:]   # strip leading relative path
    filepath = '/Users/john/python/reddit_scraper/raw/'+filepathURL
    if filepathURL[:2] == 'RC':
        filepathURL = 'http://files.pushshift.io/reddit/comments/'+filepathURL
    else:
        filepathURL = 'http://files.pushshift.io/reddit/submissions/'+filepathURL

    headers = {}
    headers['Accept'] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    headers['Accept-Encoding'] = "gzip, deflate, sdch"
    headers['Accept-Language'] = "en-US,en;q=0.8"

    r = requests.get(filepathURL,headers=headers,stream=True)
    with open(filepath,'wb') as writefile:
        for chunk in r.iter_content(1024):
            if chunk:   # filter out keep-alive new chunks
                writefile.write(chunk)
                writefile.flush()
            
    r.close()