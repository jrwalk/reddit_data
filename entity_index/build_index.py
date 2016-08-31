"""builds keyword indexer for reddit data.
"""


import pymysql as pms
import language_tools as lt
import json
import itertools


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


def build_card_db():
    """wrapper script to initialize credit-card database.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')
    cur = conn.cursor()

    with open('cards.json','r') as f:
        cards = json.load(f)['issuer']

    query = "CREATE TABLE Card_Mentions (\n"
    query += "id VARCHAR(7) PRIMARY KEY NOT NULL UNIQUE, \n"

    for issuer in cards:
        query += "{} tinyint(1) UNSIGNED DEFAULT 0, \n".format(issuer['name'])
        for card in issuer['cards']:
            query += ("{}_{} tinyint(1) UNSIGNED DEFAULT 0, \n"
                    .format(issuer['name'],card['name']))

    query = query[:-3] + ");"
    cur.execute(query)
    conn.commit()


def build_keyword_db():
    """wrapper script to initialize keyword mentions.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')
    cur = conn.cursor()

    with open('keywords.json','r') as f:
        keywords = json.load(f)['keywords']

    query = "CREATE TABLE Keywords (\n"
    query += "id VARCHAR(7) PRIMARY KEY NOT NULL UNIQUE, \n"

    for tag in keywords:
        for term in tag['terms']:
            if tag['tag'] in ['REWARDS','FEE']:
                query += ("{}_{} VARCHAR(150) DEFAULT NULL, \n"
                          .format(term['term'],tag['tag']))
            elif tag['tag'] == 'INTEREST':
                pass
            else:
                query += "{} VARCHAR(150) DEFAULT NULL, \n".format(term['term'])

    query += "INTEREST VARCHAR(150) DEFAULT NULL);"

    cur.execute(query)


def populate_card_db(results):
    """reads through list of results, populates flags in card DB.

    ARGS:
        results: list.
            list of (id,[keywords]) from `find_card_keywords`.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')
    cur = conn.cursor()

    # first, construct bank name remap - point any alt name of banks to 
    # the tagname used in the DB
    with open('banks.json','r') as f:
        banks = json.load(f)['banks']

    bank_remap = {}
    bank_lookup = {}
    for bank in banks:
        name = bank['name']
        alts = bank.get('alts',None)
        bank_lookup[name] = alts
        bank_remap[name] = name
        if alts is not None:
            for alt in alts:
                bank_remap[alt] = name

    with open('cards.json','r') as f:
        cards = json.load(f)['issuer']

    card_remap = {}
    for issuer in cards:
        issuer_name = issuer['name']
        issuer_alt_names = bank_lookup[issuer_name]
        if issuer_alt_names is not None:
            issuer_names = [issuer_name] + issuer_alt_names
        else:
            issuer_names = [issuer_name]

        for card in issuer['cards']:
            card_name = card['name']
            card_alts = card.get('alts',None)
            tagname = "{}_{}".format(issuer_name,card_name)

            if card_alts is not None:
                card_names = [card_name] + card_alts
            else:
                card_names = [card_name]

            card_keywords = itertools.product(issuer_names,card_names)
            card_keywords = ["{} {}".format(i,c) for i,c in card_keywords]

            for c in card_keywords:
                card_remap[c] = tagname

            for c in card_names:
                card_remap[c] = tagname

    # TESTING
    #for key in build_keywords():
    #    bank = bank_remap.get(key,None)
    #    card = card_remap.get(key,None)
    #    print("{}\t{}\t{}".format(key,bank,card))

    for index,keywords in results:
        tags = []
        for key in keywords:
            bank = bank_remap.get(key,None)
            card = card_remap.get(key,None)
            tags.append(bank)
            tags.append(card)

        # strip duplicates and Nones
        tags = [t for t in tags if t is not None]
        tags = set(tags)

        if len(tags) > 0:
            # write into SQL db
            # create empty row, or ignore if already exists
            cur.execute("INSERT INTO Card_Mentions (`id`) VALUES (%s) "
                        "ON DUPLICATE KEY UPDATE `id`=`id`",(index))

            query = "UPDATE Card_Mentions SET \n"
            for tag in tags:
                query += "{}=True,\n".format(tag)
            query = query[:-2] + " WHERE `id`='{}';".format(index)
            cur.execute(query)

    conn.commit()


def populate_keyword_db(results):
    """reads through list of results, populates keyword db.

    ARGS:
        results: list.
            list of (id,[keywords]) from `find_card_keywords`.
    """
    conn = pms.connect(host='localhost',
                       user='root',
                       passwd='',
                       db='reddit',
                       charset='utf8mb4',
                       init_command='SET NAMES UTF8MB4')
    cur = conn.cursor()

    # construct keyword remap:
    with open('keywords.json','r') as f:
        keywords = json.load(f)['keywords']

    tag_remap = {}
    for tag in keywords:
        name = tag['tag']
        for term in tag['terms']:
            alts = term.get('alts',None)
            if name in ['REWARDS','FEE']:
                tagname = "{}_{}".format(term['term'],name)
                kws = ["{} {}".format(term['term'],name)]
                kws.append(term['term'])
                if alts is not None:
                    kws += [alt+" "+name for alt in alts]
                    kws += alts
            elif name == "INTEREST":
                tagname = "INTEREST"
                kws = [tagname]
                kws.append(term['term'])
                if alts is not None:
                    kws += alts
                    kws += [alt+" "+name for alt in alts]
            else:
                tagname = term['term']
                kws = [tagname]
                if alts is not None:
                    kws += alts

            for kw in kws:
                tag_remap[kw] = tagname

    # TESTING
    #for key in build_keywords():
    #    tag = tag_remap.get(key,None)
    #    print("{}\t{}".format(key,tag))

    for index,keywords in results:
        tags = {}
        for key in keywords:
            tag = tag_remap.get(key,None)
            if tag is not None:
                if tag not in tags:
                    tags[tag] = [key]
                else:
                    tags[tag].append(key)

        for tag in tags.keys():
            tags[tag] = list(set(tags[tag]))    # strip duplicates
            tags[tag] = ','.join(tags[tag])

        if len(tags) > 0:
            # write into SQL db
            # create empty row, or ignore if already exists
            cur.execute("INSERT INTO Keywords (`id`) VALUES (%s) "
                        "ON DUPLICATE KEY UPDATE `id`=`id`",(index))

            query = "UPDATE Keywords SET \n"
            for tag in tags:
                query += "{}='{}',\n".format(tag,tags[tag])

            query = query[:-2] + " WHERE `id`='{}';".format(index)
            cur.execute(query)

    conn.commit()