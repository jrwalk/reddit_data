"""builds keyword indexer for reddit data.
"""


import pymysql as pms
import nltk
import language_tools as lt
import json


def get_texts(limit=None):
    """getter method for pulling comment and submission texts.

    KWARGS:
        limit: int or None.
            optional cap on number of posts to pull.

    YIELDS:
        texts: tuple.
            tuple of (id,body) for comments and submissions
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')
    cur = conn.cursor(pms.cursors.DictCursor)

    if limit is not None:
        limtext = " LIMIT {}".format(limit)
    else:
        limtext = ""

    # exclude indexing comments already in the index
    #query = """SELECT c.id, c.body FROM Comments c 
    #            WHERE NOT EXISTS (
    #                SELECT NULL FROM Mentions m 
    #                    WHERE m.id=c.id;
    #                ){};
    #        """.format(limtext)
    query = "SELECT c.id,c.body FROM Comments c{};".format(limtext)
    cur.execute(query)
    for row in cur:
        yield (row['id'],row['body'])

    #query = """SELECT s.id, s.title, s.selftext FROM Submissions s 
    #            WHERE NOT EXISTS (
    #                SELECT NULL FROM Mentions m 
    #                    WHERE m.id=s.id;
    #                ){};
    #        """.format(limtext)
    query = "SELECT s.id,s.title,s.selftext FROM Submissions s{}".format(limtext)
    cur.execute(query)
    for row in cur:
        yield (row['id'],"{} {}".format(row['title'],row['selftext']))


def build_keywords():
    """builds master list of keywords to check.

    RETURNS:
        keywords: list.
            List of strings of individual keyword terms.
    """
    with open('banks.json','r') as f:
        banks = json.load(f)['banks']

    with open('cards.json','r') as f:
        cards = json.load(f)['issuer']

    with open('keywords.json','r') as f:
        other = json.load(f)['keywords']

    keywords = []

    for bank in banks:
        keywords.append(bank['name'])
        alts = bank.get('alts',None)
        if alts is not None:
            keywords += alts

    for card in cards:
        for instance in card['cards']:
            keywords.append(instance['name'])
            alts = instance.get('alts',None)
            if alts is not None:
                keywords += alts

    for tag in other:
        for term in tag['terms']:
            keywords.append(term['term'])
            alts = term.get('alts',None)
            if alts is not None:
                keywords += alts

    for mwe in lt.multi_word_entities():
        term = ' '.join(mwe)
        keywords.append(term)

    keywords = set(keywords)

    # hack -- remove common single-word entities to avoid false positives
    removals = ['CARD','DC','FREEDOM','GOLD','GREEN','INFINITE','IT','OPEN',
                'JOURNEY','SPARK','AF FEE','INTEREST INTEREST',
                'GRACE PERIOD FEE','MINIMUM PAYMENT PAYMENT']
    keywords.difference_update(set(removals))

    return keywords


def find_card_keywords(text,keys,stopwords=None):
    """Checks text body for keywords associated with banks/cards, generates map 
    of mentioned keywords.  Lightweight output to allow for parallelization 
    before SQL write.

    ARGS:
        text: tuple.
            tuple of (id,body) output yielded by `get_texts`.
        keys: set.
            set of keywords constructed by `build_keywords`.

    RETURNS:
        map: tuple or None.
            tuple of (id,[keywords]).  If no keyword matches are found, 
            returns None.
    """
    index,body = text
    body = lt.tokenize(body,stopwords=stopwords)

    keywords = []
    for token in body:
        if token in keys:
            keywords.append(token)

    if len(keywords) > 0:
        return (index,keywords)
    else:
        return None