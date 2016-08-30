"""Processor for bank/card issuer and credit card mentions, building index 
"""


import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
import string
import json


_lemmatizer = WordNetLemmatizer()

def stop_words():
    """wrapper to return expanded set of stopwords.

    RETURNS:
        stops: set.
            set of stopword strings.
    """
    stops = stopwords.words('english')
    expansion = [
        'com',
        'http',
        'https',
        'www'
        'amp',
        'gt',
        'lt',
        'le'
    ]
    for s in string.punctuation:
        expansion.append(s)

    stops = stops+expansion
    stops = [s.upper() for s in stops]
    return set(stops)


def multi_word_entities():
    """generates set of multi-word entities to be passed to the tokenizer

    RETURNS:
        MWE: list.
            List of multi-word entities, with each entity expressed as a tuple 
            of tokens.
    """
    with open('banks.json','r') as f:
        banks = json.load(f)

    with open('cards.json','r') as f:
        cards = json.load(f)

    with open('keywords.json','r') as f:
        other = json.load(f)

    # include keywords.json here

    MWE = []

    # process bank names
    bank_lookup = {}
    for bank in banks['banks']:
        altnames = bank.get('alts',None)
        bank_lookup[bank['name']] = altnames
        if altnames is not None:
            for name in altnames:
                name = name.split()
                if len(name) > 1:
                    MWE.append(tuple(name))

    # process card names
    for issuer in cards['issuer']:
        issuer_name = issuer['name']
        issuer_alt_names = bank_lookup[issuer_name]
        for card in issuer['cards']:
            cardname = card['name']
            altnames = card.get('alts',None)

            MWE.append((issuer_name,cardname))
            if issuer_alt_names is not None:
                for ian in issuer_alt_names:
                    newname = ian.split() + [cardname]
                    MWE.append(tuple(newname))

            if altnames is not None:
                for name in altnames:
                    name = name.split()
                    if len(name) > 1:   # MWE in its own right
                        MWE.append(tuple(name))
                    else:               # MWE with the bank name
                        MWE.append((issuer_name,name[0]))
                        if issuer_alt_names is not None:
                            for ian in issuer_alt_names:
                                newname = ian.split() + name
                                MWE.append(tuple(newname))

    # process other keywords
    for tag in other['keywords']:
        tagname = tag['tag']
        for term in tag['terms']:
            if tagname != '':
                MWE.append((term['term'],tagname))
            alts = term.get('alts',None)
            if alts is not None:
                for alt in alts:
                    alt = alt.split()
                    if tagname != '':
                        MWE.append(tuple(alt + [tagname]))
                    if len(alt) > 1:
                        MWE.append(tuple(alt))

    return MWE


def tokenize(text,lemma=False,stopwords=None):
    """tokenizes the input text.

    ARGS:
        text: string.
            raw text to be tokenized.

    KWARGS:
        lemma: boolean.
            Flag to lemmatize the terms.  Default False.
        stopwords: boolean.
            Flag to remove stopwords from tokens.  Default True.

    RETURNS:
        tokens: list.
            List of upper-case tokens, represented as strings.
    """
    tokens = nltk.RegexpTokenizer(r'\w+').tokenize(text.upper())
    tokens = nltk.MWETokenizer(mwes=multi_word_entities(),
                               separator=' ').tokenize(tokens)

    if stopwords is not None:
        tokens = [word for word in tokens if word not in stopwords]

    if lemma:
        tokens = [_lemmatizer.lemmatize(word,pos='v') for word in tokens]
        tokens = [_lemmatizer.lemmatize(word,pos='n') for word in tokens]

    return tokens