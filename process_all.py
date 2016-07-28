"""main-level wrapper for downloading and processing reddit dump files.
"""

import os
from joblib import Parallel, delayed
import itertools
import read_json as r
import get_files as g
import datetime as dt


def process_all(dl_dir,subreddits,n_jobs=8):
    """process all zipped files in target dir.

    ARGS:
        dl_dir: string.
            path to directory containing zipped files.
        subreddits: list or set.
            list of strings storing the target subreddit names.
            passed to `read_json.process`.
    """
    files = os.listdir(dl_dir)
    files = [file for file in files if file[-3:] == 'bz2']
    files = [dl_dir+'/'+file for file in files]

    Parallel(n_jobs=n_jobs,verbose=5)(delayed(r.process_zip)(file,subreddits) 
                                 for file in files)


def download_set(files):
    """download all of the files specified in the input.

    ARGS:
        files: set or list.
            set or list of filenames as output by `get_files.list_files`.
            list allows for downloading subsets.
    """
    for i,file in enumerate(files):
        count = len(files)
        now = dt.datetime.now()
        g.download(file)
        then = dt.datetime.now()
        time = then - now
        print("{}/{} downloaded {}: time {}".format(i+1,count,file,time))


def run_all(subreddits,n=10,n_jobs=8):
    """wrapper function for processing.  Gets download file list, breaks into 
    chunks to avoid storage-space hogging.  Downloads each chunk, then 
    processes it (renders down to the scraped dataset and deletes the original 
    to free up space).

    ARGS:
        subreddits: list or set.
            list of strings storing target subreddit names.  
            passed to `read_json.process`.

    KWARGS:
        n: int.
            size of chunks to break down downloads.
        n_jobs: int.
            number of parallel jobs for process_all.
    """
    coms,subs = g.list_files()
    files = coms | subs     # merge sets
    files = g.restrict_files(subreddits,files)
    files = g.restrict_files_to_dir(files,
                                    '/Users/john/python/reddit_scraper/raw/')
    groups = grouper(n,files)

    print("running {} targets in batches of {}\n".format(len(files),n))

    for group in groups:
        group = [file for file in group if file is not None]
        download_set(group)
        process_all('raw',subreddits,n_jobs=n_jobs)


def grouper(n, iterable, fillvalue=None):
    """helper function to unflatten iterables.  For example,

    grouper(3, 'ABCDEFG', 'x') --> [[ABC], [DEF], [Gxx]]

    ARGS:
        n: int or float.
            size of sublists to return.
        iterable: iterable.
            flattened iterable.

    KWARGS:
        fillvalue:
            filler value to pad out lists.  Default None.
    """
    args = [iter(iterable)] * n
    groups = list(itertools.zip_longest(fillvalue=fillvalue, *args))
    return [list(el) for el in groups]