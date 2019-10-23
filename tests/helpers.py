"""
TICCLAT testing helper functions.
"""

from pathlib import Path
from itertools import chain

import nltk.data
from nltk import word_tokenize

import pandas


def load_test_db_data(dbsession):
    """
    Insert mock data into database.
    """
    files = (Path(__file__).parent / 'db_data').glob('./*.tsv')
    dbsession.execute('set foreign_key_checks=0;')
    for file in files:
        df = pandas.read_csv(file, sep='\t')
        df.to_sql(file.stem, if_exists='append', index=False, con=dbsession.bind)

    # note that the just inserted data is not checked for consistency, only new data/updates will be checked
    dbsession.execute('set foreign_key_checks=1;')


# This used to be in ticclat.tokenize, but it was no longer used anywhere but
# in some tests and in the add_wikipedia_documents notebook, so we took it out
# of the install dependencies.
def nltk_tokenize(texts_file, punkt='tokenizers/punkt/dutch.pickle'):
    """
    Inputs:
        texts_file (str): File name of a file that contains the texts. This
            should contain one document per line.
        punkt (str): Path to the nltk punctuation data to be used.

    Yields:
        Counter: term-frequency vector representing a document.
    """
    nltk.download('punkt')
    tokenizer = nltk.data.load(punkt)

    with open(texts_file) as file_handle:
        for line in file_handle:
            tokens = [word_tokenize(sent)
                      for sent in tokenizer.tokenize(line.strip())]

            yield list(chain(*tokens))
