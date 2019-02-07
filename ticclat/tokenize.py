"""Generators that produce term-frequency vectors of documents in a corpus.

A document in ticclat is a term-frequency vector (collections.Counter). This
module contains generators that return term-frequency vectors for certain types
of input data.
"""

import nltk.data

from collections import Counter
from itertools import chain

from nltk import word_tokenize


def nltk_tokenize(texts_file, punkt='tokenizers/punkt/dutch.pickle'):
    """
    Inputs:
        texts_file (str): File name of a file that contains the texts. This
            should contain one document per line.
        punkt (str): Path to the nltk punctuation data to be used.

    Yields:
        Counter: term-frequency vector representing a document.
    """
    tokenizer = nltk.data.load(punkt)

    with open(texts_file) as f:
        for line in f:
            tokens = [word_tokenize(sent)
                      for sent in tokenizer.tokenize(line.strip())]

            yield Counter(chain(*tokens))
