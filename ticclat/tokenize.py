"""Generators that produce term-frequency vectors of documents in a corpus.

A document in ticclat is a term-frequency vector (collections.Counter). This
module contains generators that return term-frequency vectors for certain types
of input data.
"""
import nltk.data

from itertools import chain

from nltk import word_tokenize

from sklearn.feature_extraction.text import CountVectorizer


def nltk_tokenize(texts_file, punkt='tokenizers/punkt/dutch.pickle'):
    """
    Inputs:
        texts_file (str): File name of a file that contains the texts. This
            should contain one document per line.
        punkt (str): Path to the nltk punctuation data to be used.

    Yields:
        Counter: term-frequency vector representing a document.
    """
    nltk.download(punkt)
    tokenizer = nltk.data.load(punkt)

    with open(texts_file) as f:
        for line in f:
            tokens = [word_tokenize(sent)
                      for sent in tokenizer.tokenize(line.strip())]

            yield list(chain(*tokens))


def do_nothing(list_of_words):
    return list_of_words


def terms_documents_matrix_counters(corpus):
    """Returns a terms document matrix corpus and related objects of a corpus

    Inputs:
        tokenizer: generator that returns an iterator of term vectors
            (Counters), e.g., nltk_tokenize
    Returns:
        corpus: a sparse terms documents matrix
        v: the vecorizer object containing the vocabulary (i.e., all word forms
            in the corpus)
    """
    v = CountVectorizer(tokenizer=do_nothing, lowercase=False)
    corpus = v.fit_transform(corpus)

    return corpus, v
